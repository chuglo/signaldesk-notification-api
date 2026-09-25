"""Real topology regression tests: migrations, constraints, and stream replay.

These fixtures intentionally use Docker directly.  Testcontainers starts a Ryuk
sidecar and may invoke credential helpers, neither of which is suitable for the
non-interactive macOS test environment.
"""
import subprocess
import time
import json
from uuid import uuid4

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from redis import Redis
from sqlalchemy import create_engine, delete, select, text
from sqlalchemy.orm import Session

from signaldesk_notification_api.models import Notification, OutboxEvent
from signaldesk_notification_api.outbox_publisher import publish_batch
from signaldesk_notification_api.schemas import AlertCreate
from signaldesk_notification_api.services import attach, claim, create_alert
from signaldesk_notification_api.main import create_app
from signaldesk_notification_api.schemas import TerminalAuthority
from signaldesk_notification_api.settings import Settings
from fastapi.testclient import TestClient

POSTGRES_IMAGE = "postgres:16-alpine@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777"
REDIS_IMAGE = "redis:7-alpine@sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99"
_STARTUP_TIMEOUT_SECONDS = 30


def _docker(*args: str) -> str:
    completed = subprocess.run(
        ["docker", *args], check=True, text=True, capture_output=True, timeout=15
    )
    return completed.stdout.strip()


def _mapped_port(container_id: str, container_port: int) -> int | None:
    try:
        mapping = _docker("port", container_id, f"{container_port}/tcp")
    except subprocess.CalledProcessError:
        return None
    if not mapping:
        return None
    return int(mapping.rsplit(":", 1)[1])


def _start_container(*, image: str, name_prefix: str, port: int, env: tuple[str, ...] = ()) -> tuple[str, int]:
    # Inspecting first is deliberate: --pull=never prevents all registry and
    # credential-helper activity, and gives a useful failure for missing images.
    _docker("image", "inspect", image)
    name = f"signaldesk-notification-api-{name_prefix}-{uuid4().hex}"
    container_id = _docker(
        "run", "--pull=never", "--rm", "-d", "-P", "--name", name,
        *[item for variable in env for item in ("-e", variable)], image,
    )
    try:
        deadline = time.monotonic() + _STARTUP_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            mapped = _mapped_port(container_id, port)
            if mapped is not None:
                return container_id, mapped
            time.sleep(0.1)
        raise TimeoutError(f"Docker did not map port {port} for {container_id}")
    except BaseException:
        _remove_container(container_id)
        raise


def _remove_container(container_id: str) -> None:
    # Exact IDs make cleanup safe even when multiple test runs coexist.
    subprocess.run(["docker", "rm", "-f", container_id], check=False, capture_output=True, timeout=15)


def _wait_for_postgres(port: int) -> str:
    url = f"postgresql+psycopg://postgres:postgres@127.0.0.1:{port}/postgres"
    deadline = time.monotonic() + _STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(url.replace("+psycopg", ""), connect_timeout=1) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
            return url
        except psycopg.OperationalError:
            time.sleep(0.2)
    raise TimeoutError("PostgreSQL did not become ready")


def _wait_for_redis(port: int) -> Redis:
    client = Redis(host="127.0.0.1", port=port, decode_responses=True, socket_connect_timeout=1, socket_timeout=1)
    deadline = time.monotonic() + _STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            if client.ping():
                return client
        except Exception:  # Redis raises its own connection exception hierarchy.
            time.sleep(0.2)
    client.close()
    raise TimeoutError("Redis did not become ready")


@pytest.fixture(scope="module")
def postgres_url():
    container_id: str | None = None
    try:
        container_id, port = _start_container(
            image=POSTGRES_IMAGE, name_prefix="postgres", port=5432,
            env=("POSTGRES_PASSWORD=postgres",),
        )
        yield _wait_for_postgres(port)
    finally:
        if container_id is not None:
            _remove_container(container_id)


@pytest.fixture(scope="module")
def redis_client():
    container_id: str | None = None
    client: Redis | None = None
    try:
        container_id, port = _start_container(image=REDIS_IMAGE, name_prefix="redis", port=6379)
        client = _wait_for_redis(port)
        yield client
    finally:
        if client is not None:
            client.close()
        if container_id is not None:
            _remove_container(container_id)


def alert() -> AlertCreate:
    ids = [uuid4() for _ in range(6)]
    return AlertCreate(organization_id=ids[0], correlation_id=ids[1], monitor_id=ids[2], monitor_run_id=ids[3], diagnostic_job_id=ids[4], requested_by_user_id=ids[5], template_name="diagnostic_alert", template_data={"monitor_id": ids[2], "monitor_run_id": ids[3], "diagnostic_job_id": ids[4], "status": "completed", "outcome": "error", "error_code": "connection_timeout"})


class _Authority:
    def __init__(self, request: AlertCreate) -> None:
        self.value = TerminalAuthority(diagnostic_job_id=request.diagnostic_job_id, organization_id=request.organization_id, requested_by_user_id=request.requested_by_user_id, correlation_id=request.correlation_id, status=request.template_data.status, outcome=request.template_data.outcome, error_code=request.template_data.error_code, notification_mode="alert_rule")

    def terminal_authority(self, _diagnostic_job_id): return self.value
    def ready(self): return None
    def close(self): return None


def test_real_postgres_migration_constraints_and_lease(postgres_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    command.upgrade(config, "head")
    engine = create_engine(postgres_url)
    with Session(engine) as session:
        created = create_alert(session, alert()); session.commit()
        leased = claim(session, created["id"], 60); session.commit()
        assert leased["lease_generation"] == 1
        attach(session, created["id"], leased["lease_token"], leased["lease_generation"], uuid4()); session.commit()
        with pytest.raises(Exception):
            session.execute(text("UPDATE notifications SET state = 'email_attached', email_delivery_id = NULL WHERE id = :id"), {"id": created["id"]})
            session.commit()
        session.rollback()
    engine.dispose()


def test_real_postgres_forged_tenant_is_rejected_before_outbox(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    request = alert()
    settings = Settings(database_url=postgres_url, redis_url="redis://localhost/0", alert_rule_worker_service_credential="a" * 32, notification_worker_service_credential="b" * 32, control_api_url="http://control.test", control_api_credential="c" * 32)
    app = create_app(settings=settings, session_factory=__import__("sqlalchemy.orm", fromlist=["sessionmaker"]).sessionmaker(bind=engine, expire_on_commit=False), control_client=_Authority(request))
    body = request.model_dump(mode="json"); body["organization_id"] = str(uuid4())
    headers = {"X-SignalDesk-Service-Actor": "alert-rule-worker", "X-SignalDesk-Service-Credential": "a" * 32}
    try:
        with Session(engine) as session:
            session.execute(delete(OutboxEvent)); session.execute(delete(Notification)); session.commit()
        with TestClient(app) as client:
            assert client.post("/internal/notifications/alerts", json=body, headers=headers).status_code == 409
        with Session(engine) as session:
            assert session.scalar(select(text("count(*)")).select_from(Notification)) == 0
            assert session.scalar(select(text("count(*)")).select_from(OutboxEvent)) == 0
    finally:
        engine.dispose()


def test_real_redis_stream_marker_replay_and_mismatch(postgres_url: str, redis_client: Redis) -> None:
    engine = create_engine(postgres_url)
    stream = "signaldesk:notifications"
    with Session(engine) as session:
        # The lease test intentionally leaves its durable outbox record behind.
        # Isolate this bounded-publish scenario so its first item is known.
        session.execute(delete(OutboxEvent))
        session.execute(delete(Notification))
        session.commit()
        redis_client.delete(stream)
        created = create_alert(session, alert()); session.commit()
        assert publish_batch(session=session, redis_client=redis_client, batch_size=1) == 1
        row = session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == created["id"]))
        assert row is not None
        entries = redis_client.xrange(stream)
        assert len(entries) == 1 and set(entries[0][1]) == {"event", "event_id"}
        row.published_at = None; session.commit()
        assert publish_batch(session=session, redis_client=redis_client, batch_size=1) == 1
        assert len(redis_client.xrange(stream)) == 1
        row.published_at = None; row.next_attempt_at = None; session.commit()
        redis_client.set(f"signaldesk:outbox:published:{row.id}", "signaldesk:notifications|0-0")
        assert publish_batch(session=session, redis_client=redis_client, batch_size=1) == 0
        session.refresh(row)
        assert row.published_at is None and row.next_attempt_at is not None and row.attempt_count == 1
        redis_client.delete(stream, f"signaldesk:outbox:published:{row.id}")
    engine.dispose()


def test_real_redis_caught_up_tail_is_bounded_and_envelope_is_exact(postgres_url: str, redis_client: Redis) -> None:
    engine = create_engine(postgres_url)
    stream = "signaldesk:notifications"
    group = "caught-up-retention"
    markers: list[str] = []
    redis_client.delete(stream)
    try:
        redis_client.xgroup_create(stream, group, id="0-0", mkstream=True)
        with Session(engine) as session:
            session.execute(delete(OutboxEvent)); session.execute(delete(Notification)); session.commit()
            rows = []
            for _ in range(3):
                created = create_alert(session, alert()); session.commit()
                row = session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == created["id"]))
                assert row is not None
                rows.append(row)
                markers.append(f"signaldesk:outbox:published:{row.id}")
                assert publish_batch(session=session, redis_client=redis_client, batch_size=1, stream_maxlen=2) == 1
                delivered = redis_client.xreadgroup(group, "retention-test", {stream: ">"}, count=10)
                entry_ids = [entry_id for _name, entries in delivered for entry_id, _fields in entries]
                assert entry_ids and redis_client.xack(stream, group, *entry_ids) == len(entry_ids)
            entries = redis_client.xrange(stream)
            assert len(entries) == 2
            fields = entries[-1][1]
            assert set(fields) == {"event", "event_id"}
            assert fields["event_id"] == str(rows[-1].id)
            assert json.loads(fields["event"]) == rows[-1].payload_json
            marker_ttl = redis_client.ttl(f"signaldesk:outbox:published:{rows[-1].id}")
            assert 0 < marker_ttl <= 7 * 24 * 60 * 60
            info = redis_client.xinfo_groups(stream)
            assert len(info) == 1 and info[0]["pending"] == 0 and info[0]["lag"] == 0
            # A response-loss recovery sees its marker before retention logic, so it
            # must not trim even a caught-up stream with a lower requested bound.
            rows[-1].published_at = None; session.commit()
            assert publish_batch(session=session, redis_client=redis_client, batch_size=1, stream_maxlen=1) == 1
            assert redis_client.xlen(stream) == 2
    finally:
        redis_client.delete(stream, *markers)
        engine.dispose()


def test_real_redis_does_not_trim_pending_or_unread_streams(postgres_url: str, redis_client: Redis) -> None:
    engine = create_engine(postgres_url)
    stream = "signaldesk:notifications"
    redis_client.delete(stream)
    marker = None
    try:
        old_id = redis_client.xadd(stream, {"event": "old", "event_id": "old"})
        redis_client.xgroup_create(stream, "pending-group", id="$")
        pending_id = redis_client.xadd(stream, {"event": "pending", "event_id": "pending"})
        redis_client.xreadgroup("pending-group", "consumer", {stream: ">"}, count=1)
        redis_client.xgroup_create(stream, "unread-group", id=old_id)
        with Session(engine) as session:
            session.execute(delete(OutboxEvent)); session.execute(delete(Notification)); session.commit()
            created = create_alert(session, alert()); session.commit()
            row = session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == created["id"]))
            assert row is not None
            marker = f"signaldesk:outbox:published:{row.id}"
            assert publish_batch(session=session, redis_client=redis_client, batch_size=1, stream_maxlen=1) == 1
        entries = redis_client.xrange(stream)
        assert [entry_id for entry_id, _fields in entries][:2] == [old_id, pending_id]
        assert len(entries) == 3
        groups = {item["name"]: item for item in redis_client.xinfo_groups(stream)}
        assert groups["pending-group"]["pending"] == 1
        assert groups["unread-group"]["lag"] > 0
    finally:
        redis_client.delete(stream, *( [marker] if marker else [] ))
        engine.dispose()


def test_real_redis_response_loss_replays_without_duplicate_before_marker_ttl(postgres_url: str, redis_client: Redis) -> None:
    class ResponseLostRedis:
        def __init__(self, client: Redis) -> None:
            self.client = client
            self.lost = True

        def info(self, section: str):
            return self.client.info(section)

        def eval(self, *args):
            result = self.client.eval(*args)
            if self.lost:
                self.lost = False
                raise ConnectionError("simulated Redis response loss after execution")
            return result

    engine = create_engine(postgres_url)
    stream = "signaldesk:notifications"
    redis_client.delete(stream)
    marker = None
    try:
        with Session(engine) as session:
            session.execute(delete(OutboxEvent)); session.execute(delete(Notification)); session.commit()
            created = create_alert(session, alert()); session.commit()
            row = session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == created["id"]))
            assert row is not None
            marker = f"signaldesk:outbox:published:{row.id}"
            assert publish_batch(session=session, redis_client=ResponseLostRedis(redis_client), batch_size=1) == 0
            assert redis_client.xlen(stream) == 1 and redis_client.ttl(marker) > 0
            session.refresh(row)
            row.next_attempt_at = None; session.commit()
            assert publish_batch(session=session, redis_client=redis_client, batch_size=1) == 1
            assert redis_client.xlen(stream) == 1
    finally:
        redis_client.delete(stream, *( [marker] if marker else [] ))
        engine.dispose()
