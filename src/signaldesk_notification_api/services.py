"""Durable notification state transitions; lease capabilities are never persisted raw."""
import hashlib
import json
import secrets
from datetime import timedelta
from uuid import UUID, uuid4

from fastapi import HTTPException, status
from signaldesk_contracts import NotificationRequestedV1
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import Notification, OutboxEvent
from .schemas import AlertCreate, TemplateData


def _fingerprint(request: AlertCreate) -> str:
    # Canonical identity protects correlation replay and makes tenant substitution impossible.
    return hashlib.sha256(json.dumps(request.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _response(row: Notification) -> dict:
    return {"id": row.id, "organization_id": row.organization_id, "correlation_id": row.correlation_id,
            "state": row.state, "monitor_id": row.monitor_id, "monitor_run_id": row.monitor_run_id,
            "diagnostic_job_id": row.diagnostic_job_id, "requested_by_user_id": row.requested_by_user_id,
            "template_name": row.template_name, "template_data": TemplateData.model_validate_json(json.dumps(row.template_data_json)),
            "email_delivery_id": row.email_delivery_id, "failure_code": row.failure_code,
            "lease_generation": row.lease_generation, "lease_expires_at": row.lease_expires_at}


def create_alert(session: Session, request: AlertCreate) -> dict:
    fingerprint = _fingerprint(request)
    existing = session.scalar(select(Notification).where(Notification.organization_id == request.organization_id, Notification.correlation_id == request.correlation_id, Notification.kind == "diagnostic_alert").with_for_update())
    if existing:
        if not secrets.compare_digest(existing.payload_fingerprint, fingerprint):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="notification replay conflicts with existing request")
        return _response(existing)
    row = Notification(id=uuid4(), organization_id=request.organization_id, correlation_id=request.correlation_id,
        monitor_id=request.monitor_id, monitor_run_id=request.monitor_run_id, diagnostic_job_id=request.diagnostic_job_id,
        requested_by_user_id=request.requested_by_user_id, template_data_json=request.template_data.model_dump(mode="json"), payload_fingerprint=fingerprint)
    event = OutboxEvent(id=uuid4(), event_type="notification.requested.v1", aggregate_id=row.id, payload_json={})
    event.payload_json = NotificationRequestedV1(schema_version=1, event_type="notification.requested.v1", event_id=event.id,
        occurred_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc), correlation_id=row.correlation_id,
        organization_id=row.organization_id, notification_id=row.id).model_dump(mode="json")
    session.add_all((row, event))
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        existing = session.scalar(select(Notification).where(Notification.organization_id == request.organization_id, Notification.correlation_id == request.correlation_id, Notification.kind == "diagnostic_alert"))
        if existing and secrets.compare_digest(existing.payload_fingerprint, fingerprint):
            return _response(existing)
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="notification replay conflicts with existing request")
    return _response(row)


def claim(session: Session, notification_id: UUID, lease_seconds: int) -> dict:
    row = session.scalar(select(Notification).where(Notification.id == notification_id).with_for_update())
    if not row:
        raise HTTPException(404, "notification not found")
    now = session.scalar(select(func.now()))
    if row.state in ("email_attached", "failed"):
        return _response(row)
    if row.state in ("claimed", "email_attached") and row.lease_expires_at and row.lease_expires_at > now:
        raise HTTPException(409, "notification is leased")
    token = secrets.token_urlsafe(32)
    row.lease_token_hash = hashlib.sha256(token.encode()).hexdigest()
    row.lease_generation += 1
    row.lease_expires_at = now + timedelta(seconds=lease_seconds)
    row.state = "claimed"
    response = _response(row)
    response["lease_token"] = token
    return response


def _leased(session: Session, notification_id: UUID, token: str, generation: int) -> Notification:
    row = session.scalar(select(Notification).where(Notification.id == notification_id).with_for_update())
    if not row:
        raise HTTPException(404, "notification not found")
    now = session.scalar(select(func.now()))
    digest = hashlib.sha256(token.encode()).hexdigest()
    if row.state in ("email_attached", "failed"):
        raise HTTPException(409, "terminal notification cannot change")
    if generation != row.lease_generation or not row.lease_token_hash or not secrets.compare_digest(row.lease_token_hash, digest) or not row.lease_expires_at or row.lease_expires_at <= now:
        raise HTTPException(409, "notification lease is no longer valid")
    return row


def attach(session: Session, notification_id: UUID, token: str, generation: int, delivery_id: UUID) -> dict:
    row = session.scalar(select(Notification).where(Notification.id == notification_id).with_for_update())
    if not row: raise HTTPException(404, "notification not found")
    if row.state == "email_attached":
        if row.email_delivery_id == delivery_id: return _response(row)
        raise HTTPException(409, "terminal notification cannot change delivery")
    if row.state == "failed": raise HTTPException(409, "terminal notification cannot change delivery")
    # First mutation validates both capability components against server time.
    now = session.scalar(select(func.now()))
    digest = hashlib.sha256(token.encode()).hexdigest()
    if generation != row.lease_generation or not row.lease_token_hash or not secrets.compare_digest(row.lease_token_hash, digest) or not row.lease_expires_at or row.lease_expires_at <= now:
        raise HTTPException(409, "notification lease is no longer valid")
    row.email_delivery_id = delivery_id; row.state = "email_attached"; row.lease_token_hash = None; row.lease_expires_at = None
    return _response(row)


def mark_failed(session: Session, notification_id: UUID, token: str, generation: int, code: str) -> dict:
    row = session.scalar(select(Notification).where(Notification.id == notification_id).with_for_update())
    if not row: raise HTTPException(404, "notification not found")
    if row.state == "failed":
        if row.failure_code == code: return _response(row)
        raise HTTPException(409, "failure code conflicts")
    if row.state == "email_attached": raise HTTPException(409, "attached notification cannot fail")
    now = session.scalar(select(func.now())); digest = hashlib.sha256(token.encode()).hexdigest()
    if generation != row.lease_generation or not row.lease_token_hash or not secrets.compare_digest(row.lease_token_hash, digest) or not row.lease_expires_at or row.lease_expires_at <= now:
        raise HTTPException(409, "notification lease is no longer valid")
    row.state = "failed"; row.failure_code = code; row.lease_token_hash = None; row.lease_expires_at = None
    return _response(row)
