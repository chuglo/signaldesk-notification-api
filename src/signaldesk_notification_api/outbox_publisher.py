"""Bounded, replay-safe publisher for notification outbox rows."""
import argparse
import json
from datetime import timedelta
import signal
import time
from uuid import UUID

from redis import Redis
from signaldesk_contracts import REDIS_STREAM_BY_EVENT_TYPE, parse_event_json
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from .database import create_engine, create_session_factory
from .models import OutboxEvent
from .settings import OutboxPublisherSettings

_LUA = """
local marker=redis.call('GET',KEYS[1])
if marker then
 local p=string.find(marker,'|',1,true); if not p then return {-1,'marker'} end
 local stream=string.sub(marker,1,p-1); local id=string.sub(marker,p+1)
 if stream~=KEYS[2] then return {-2,'stream'} end
 local rows=redis.call('XRANGE',KEYS[2],id,id); if #rows~=1 then return {-3,'entry'} end
 local f=rows[1][2]
 if #f~=4 or f[1]~='event' or f[2]~=ARGV[1] or f[3]~='event_id' or f[4]~=ARGV[2] then return {-4,'payload'} end
 return {0,id}
end
local groups=redis.pcall('XINFO','GROUPS',KEYS[2])
local caught_up=type(groups)=='table' and groups.err==nil and #groups>0
if caught_up then
 for g=1,#groups do
  local pending=nil; local lag=nil; local values=groups[g]
  for n=1,#values,2 do
   if values[n]=='pending' then pending=values[n+1] elseif values[n]=='lag' then lag=values[n+1] end
  end
  if pending==nil or lag==nil or tonumber(pending)==nil or tonumber(lag)==nil or tonumber(pending)~=0 or tonumber(lag)~=0 then caught_up=false; break end
 end
end
if caught_up then redis.call('XTRIM',KEYS[2],'MAXLEN','=',tonumber(ARGV[3])-1) end
local id=redis.call('XADD',KEYS[2],'*','event',ARGV[1],'event_id',ARGV[2]); redis.call('SET',KEYS[1],KEYS[2]..'|'..id,'EX',ARGV[4]); return {1,id}
"""

DEFAULT_STREAM_MAXLEN = 10_000
DEFAULT_REPLAY_MARKER_TTL_SECONDS = 7 * 24 * 60 * 60

def _standalone(redis: Redis) -> None:
    info = redis.info("cluster")
    if not isinstance(info, dict) or info.get("cluster_enabled") not in (0, "0", False): raise RuntimeError("outbox publication requires standalone Redis")

def _canonical(row: OutboxEvent) -> str:
    event = parse_event_json(json.dumps(row.payload_json, sort_keys=True, separators=(",", ":")))
    if event.event_type != "notification.requested.v1" or event.event_id != row.id or event.notification_id != row.aggregate_id: raise ValueError("malformed outbox row")
    return json.dumps(event.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))

def _retry(session: Session, row_id: UUID) -> None:
    row = session.scalar(select(OutboxEvent).where(OutboxEvent.id == row_id, OutboxEvent.published_at.is_(None)).with_for_update())
    if row:
        row.attempt_count += 1; now = session.scalar(select(func.now())); row.next_attempt_at = now + timedelta(seconds=min(300, 30 * 2 ** min(row.attempt_count - 1, 3))); session.commit()

def publish_batch(*, session: Session, redis_client: Redis, batch_size: int = 100,
                  stream_maxlen: int = DEFAULT_STREAM_MAXLEN,
                  replay_marker_ttl_seconds: int = DEFAULT_REPLAY_MARKER_TTL_SECONDS) -> int:
    if not 1 <= batch_size <= 100: raise ValueError("batch_size must be between 1 and 100")
    if type(stream_maxlen) is not int or not 1 <= stream_maxlen <= 1_000_000: raise ValueError("stream_maxlen must be between 1 and 1000000")
    if type(replay_marker_ttl_seconds) is not int or not 60 <= replay_marker_ttl_seconds <= 2_592_000: raise ValueError("replay_marker_ttl_seconds must be between 60 and 2592000")
    _standalone(redis_client); published = 0; attempted: set[UUID] = set()
    for _ in range(batch_size):
        row = session.scalar(select(OutboxEvent).where(OutboxEvent.published_at.is_(None), or_(OutboxEvent.next_attempt_at.is_(None), OutboxEvent.next_attempt_at <= func.now()), OutboxEvent.id.not_in(attempted) if attempted else True).order_by(OutboxEvent.created_at, OutboxEvent.id).with_for_update(skip_locked=True).limit(1))
        if not row: session.rollback(); break
        attempted.add(row.id)
        try:
            payload = _canonical(row); stream = REDIS_STREAM_BY_EVENT_TYPE["notification.requested.v1"]
            result = redis_client.eval(_LUA, 2, f"signaldesk:outbox:published:{row.id}", stream, payload, str(row.id), stream_maxlen, replay_marker_ttl_seconds)
            if not isinstance(result, (list, tuple)) or len(result) != 2 or int(result[0]) not in (0, 1): raise RuntimeError("Redis did not confirm outbox publication")
            row.published_at = session.scalar(select(func.now())); row.next_attempt_at = None; session.commit(); published += 1
        except Exception:
            session.rollback(); _retry(session, row.id)
    return published

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--once", action="store_true"); parser.add_argument("--batch-size", type=int, default=100); parser.add_argument("--poll-interval", type=float, default=1.0); parser.add_argument("--stream-maxlen", type=int, default=DEFAULT_STREAM_MAXLEN); parser.add_argument("--replay-marker-ttl-seconds", type=int, default=DEFAULT_REPLAY_MARKER_TTL_SECONDS); args = parser.parse_args(argv)
    if not 0.1 <= args.poll_interval <= 60: parser.error("--poll-interval must be between 0.1 and 60")
    if not 1 <= args.stream_maxlen <= 1_000_000: parser.error("--stream-maxlen must be between 1 and 1000000")
    if not 60 <= args.replay_marker_ttl_seconds <= 2_592_000: parser.error("--replay-marker-ttl-seconds must be between 60 and 2592000")
    settings = OutboxPublisherSettings(); engine = create_engine(settings); client = Redis.from_url(settings.redis_url.unicode_string(), socket_connect_timeout=2, socket_timeout=2)
    try:
        factory = create_session_factory(engine)
        if args.once:
            with factory() as session: publish_batch(session=session, redis_client=client, batch_size=args.batch_size, stream_maxlen=args.stream_maxlen, replay_marker_ttl_seconds=args.replay_marker_ttl_seconds)
            return 0
        stopping = False
        def stop(*_args):
            nonlocal stopping
            stopping = True
        old_int, old_term = signal.signal(signal.SIGINT, stop), signal.signal(signal.SIGTERM, stop)
        try:
            while not stopping:
                with factory() as session: publish_batch(session=session, redis_client=client, batch_size=args.batch_size, stream_maxlen=args.stream_maxlen, replay_marker_ttl_seconds=args.replay_marker_ttl_seconds)
                if not stopping: time.sleep(args.poll_interval)
        finally:
            signal.signal(signal.SIGINT, old_int); signal.signal(signal.SIGTERM, old_term)
    finally: engine.dispose(); client.close()
