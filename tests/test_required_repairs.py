"""Regression boundaries required for the notification authority."""
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from signaldesk_notification_api.models import Base, Notification
from signaldesk_notification_api.schemas import AlertCreate
from signaldesk_notification_api.services import attach, claim, create_alert, mark_failed


def payload() -> AlertCreate:
    ids = [uuid4() for _ in range(6)]
    return AlertCreate(
        organization_id=ids[0], correlation_id=ids[1], monitor_id=ids[2],
        monitor_run_id=ids[3], diagnostic_job_id=ids[4], requested_by_user_id=ids[5],
        template_name="diagnostic_alert", template_data={
            "monitor_id": ids[2], "monitor_run_id": ids[3],
            "diagnostic_job_id": ids[4], "status": "completed", "outcome": "error",
            "error_code": "connection_timeout",
        },
    )


@pytest.fixture
def session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        yield db


@pytest.mark.parametrize("mutation", [
    {"status": "completed", "outcome": "reachable"},
    {"status": "completed", "outcome": None},
    {"error_code": "bad-code"}, {"error_code": "bad\r\ncode"},
    {"target": "https://attacker.test"},
])
def test_alert_template_matches_email_worker_contract(mutation: dict[str, object]) -> None:
    candidate = payload().model_dump(mode="json")
    candidate["template_data"].update(mutation)
    with pytest.raises(ValueError):
        AlertCreate.model_validate(candidate)


def test_failed_alert_allows_null_outcome_and_error_code() -> None:
    candidate = payload().model_dump(mode="json")
    candidate["template_data"].update(status="failed", outcome=None, error_code=None)
    assert AlertCreate.model_validate(candidate).template_data.outcome is None


def test_attach_is_terminal_and_replay_safe(session: Session) -> None:
    notification = create_alert(session, payload()); session.commit()
    leased = claim(session, notification["id"], 60); session.commit()
    delivery = uuid4()
    result = attach(session, notification["id"], leased["lease_token"], leased["lease_generation"], delivery)
    session.commit()
    assert result["state"] == "email_attached"
    row = session.get(Notification, notification["id"])
    assert row.lease_token_hash is None and row.lease_expires_at is None
    replay = attach(session, notification["id"], "not-a-live-token", 0, delivery)
    assert replay["email_delivery_id"] == delivery
    with pytest.raises(HTTPException, match="terminal"):
        attach(session, notification["id"], "not-a-live-token", 0, uuid4())
    with pytest.raises(HTTPException):
        mark_failed(session, notification["id"], "not-a-live-token", 0, "delivery_failed")


def test_claim_terminal_never_discloses_raw_token(session: Session) -> None:
    notification = create_alert(session, payload()); session.commit()
    leased = claim(session, notification["id"], 60); session.commit()
    attach(session, notification["id"], leased["lease_token"], leased["lease_generation"], uuid4()); session.commit()
    terminal = claim(session, notification["id"], 60)
    assert "lease_token" not in terminal and terminal["state"] == "email_attached"


def test_dockerfile_is_reproducible_and_uses_relative_build_contexts() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
    assert "python:3.11.15-slim-bookworm@sha256:b18992999dbe963a45a8a4da40ac2b1975be1a776d939d098c647482bcad5cba" in dockerfile
    assert "uv==0.11.31" in dockerfile and "uv sync --locked --no-dev --no-editable" in dockerfile
    assert "--from=contracts" in dockerfile and "--from=service-kit" in dockerfile and "USER 10001:10001" in dockerfile


def test_migration_and_model_have_no_unsupported_sent_state() -> None:
    migration = Path("alembic/versions/20260825_0001_initial.py").read_text(encoding="utf-8")
    assert "'sent'" not in migration
