from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON, Uuid


class Base(DeclarativeBase):
    pass


JSON_TYPE = JSON().with_variant(JSONB, "postgresql")


class Notification(Base):
    __tablename__ = "notifications"
    __table_args__ = (
        UniqueConstraint("organization_id", "correlation_id", "kind", name="uq_notifications_organization_correlation_kind"),
        CheckConstraint("state IN ('pending','claimed','email_attached','failed')", name="ck_notifications_state"),
        CheckConstraint("kind = 'diagnostic_alert'", name="ck_notifications_kind"),
        CheckConstraint("template_name = 'diagnostic_alert'", name="ck_notifications_template"),
        CheckConstraint("lease_generation >= 0", name="ck_notifications_lease_generation"),
        CheckConstraint("failure_code IS NULL OR failure_code IN ('recipient_unavailable','invalid_template','delivery_failed','member_removed')", name="ck_notifications_failure_code"),
        CheckConstraint("(state = 'claimed' AND lease_token_hash IS NOT NULL AND lease_expires_at IS NOT NULL AND email_delivery_id IS NULL AND failure_code IS NULL) OR (state = 'email_attached' AND lease_token_hash IS NULL AND lease_expires_at IS NULL AND email_delivery_id IS NOT NULL AND failure_code IS NULL) OR (state = 'failed' AND lease_token_hash IS NULL AND lease_expires_at IS NULL AND email_delivery_id IS NULL AND failure_code IS NOT NULL) OR (state = 'pending' AND lease_token_hash IS NULL AND lease_expires_at IS NULL AND email_delivery_id IS NULL AND failure_code IS NULL)", name="ck_notifications_state_coherence"),
        Index("ix_notifications_claimable", "state", "lease_expires_at", "created_at"),
    )
    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    organization_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    correlation_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False, default="diagnostic_alert")
    monitor_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    monitor_run_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    diagnostic_job_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    requested_by_user_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    template_name: Mapped[str] = mapped_column(String(64), nullable=False, default="diagnostic_alert")
    template_data_json: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, nullable=False)
    payload_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="pending")
    lease_token_hash: Mapped[str | None] = mapped_column(String(64))
    lease_generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    email_delivery_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True))
    failure_code: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        UniqueConstraint("event_type", "aggregate_id", name="uq_outbox_events_type_aggregate"),
        CheckConstraint("attempt_count >= 0", name="ck_outbox_events_attempt_count"),
        Index("ix_outbox_events_publication_eligibility", "published_at", "next_attempt_at", "created_at", "id"),
    )
    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
