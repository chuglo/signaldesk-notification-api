from datetime import datetime, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from signaldesk_notification_api.models import Base, Notification, OutboxEvent
from signaldesk_notification_api.schemas import AlertCreate
from signaldesk_notification_api.services import attach, claim, create_alert, mark_failed
from signaldesk_notification_api.main import create_app
from signaldesk_notification_api.outbox_publisher import publish_batch
from signaldesk_notification_api.settings import Settings
from signaldesk_notification_api.schemas import TerminalAuthority


def test_notification_api_package_is_importable() -> None:
    from signaldesk_notification_api.main import create_app

    assert create_app is not None


def payload() -> AlertCreate:
    ids = [uuid4() for _ in range(6)]
    return AlertCreate(organization_id=ids[0], correlation_id=ids[1], monitor_id=ids[2], monitor_run_id=ids[3], diagnostic_job_id=ids[4], requested_by_user_id=ids[5], template_name="diagnostic_alert", template_data={"monitor_id": ids[2], "monitor_run_id": ids[3], "diagnostic_job_id": ids[4], "status": "failed", "outcome": None, "error_code": None})


class FakeControl:
    def __init__(self, authority: TerminalAuthority | None = None, error: Exception | None = None) -> None:
        self.authority = authority
        self.error = error
        self.closed = False
        self.ready_calls = 0

    def terminal_authority(self, _diagnostic_job_id):
        if self.error:
            raise self.error
        assert self.authority is not None
        return self.authority

    def ready(self) -> None:
        self.ready_calls += 1
        if self.error:
            raise self.error

    def close(self) -> None:
        self.closed = True


def authority(request: AlertCreate) -> TerminalAuthority:
    return TerminalAuthority(diagnostic_job_id=request.diagnostic_job_id, organization_id=request.organization_id, requested_by_user_id=request.requested_by_user_id, correlation_id=request.correlation_id, status=request.template_data.status, outcome=request.template_data.outcome, error_code=request.template_data.error_code, notification_mode="alert_rule")


@pytest.fixture
def session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        yield db


def test_replay_creates_exactly_one_notification_and_outbox(session: Session) -> None:
    request = payload()
    created = create_alert(session, request); session.commit()
    replay = create_alert(session, request); session.commit()
    assert created["id"] == replay["id"]
    assert len(session.scalars(select(Notification)).all()) == 1
    assert len(session.scalars(select(OutboxEvent)).all()) == 1


def test_divergent_replay_is_conflict(session: Session) -> None:
    request = payload(); create_alert(session, request); session.commit()
    changed = request.model_copy(update={"requested_by_user_id": uuid4()})
    with pytest.raises(HTTPException, match="conflicts") as error: create_alert(session, changed)
    assert error.value.status_code == 409


def test_claim_attach_terminal_monotonicity(session: Session) -> None:
    notification = create_alert(session, payload()); session.commit()
    leased = claim(session, notification["id"], 60); session.commit()
    delivery = uuid4()
    assert attach(session, notification["id"], leased["lease_token"], leased["lease_generation"], delivery)["state"] == "email_attached"; session.commit()
    assert attach(session, notification["id"], "response-loss-token", 0, delivery)["email_delivery_id"] == delivery; session.commit()
    with pytest.raises(HTTPException) as error: mark_failed(session, notification["id"], leased["lease_token"], leased["lease_generation"], "delivery_failed")
    assert error.value.status_code == 409


def test_stale_lease_is_rejected_after_reclaim(session: Session) -> None:
    notification = create_alert(session, payload()); session.commit()
    first = claim(session, notification["id"], 60); session.commit()
    row = session.get(Notification, notification["id"]); row.lease_expires_at = datetime(2000, 1, 1, tzinfo=timezone.utc); session.commit()
    second = claim(session, notification["id"], 60); session.commit()
    with pytest.raises(HTTPException) as error: attach(session, notification["id"], first["lease_token"], first["lease_generation"], uuid4())
    assert error.value.status_code == 409 and first["lease_token"] != second["lease_token"]


def test_settings_require_distinct_credentials() -> None:
    common = "a" * 32
    with pytest.raises(ValueError, match="distinct"):
        Settings(database_url="postgresql://user:pass@localhost/db", redis_url="redis://localhost/0", alert_rule_worker_service_credential=common, notification_worker_service_credential=common, control_api_url="http://control.test", control_api_credential="c" * 32)


def test_authentication_and_liveness_are_isolated(session: Session) -> None:
    settings = Settings(database_url="postgresql://user:pass@localhost/db", redis_url="redis://localhost/0", alert_rule_worker_service_credential="a" * 32, notification_worker_service_credential="b" * 32, control_api_url="http://control.test", control_api_credential="c" * 32)
    request = payload()
    app = create_app(settings=settings, session_factory=sessionmaker(bind=session.get_bind(), expire_on_commit=False), control_client=FakeControl(authority(request)))
    client = TestClient(app)
    assert client.get("/healthz").status_code == 200
    body = request.model_dump(mode="json")
    assert client.post("/internal/notifications/alerts", json=body).status_code == 401
    # Dependencies run before body validation, so an unauthenticated attacker cannot
    # use validation details as an oracle for a payload they control.
    malformed = client.post("/internal/notifications/alerts", json={"recipient_email": "private@example.test"})
    assert malformed.status_code == 401 and "recipient_email" not in malformed.text
    denied = client.post("/internal/notifications/alerts", json=body, headers={"X-SignalDesk-Service-Actor": "notification-worker", "X-SignalDesk-Service-Credential": "b" * 32})
    assert denied.status_code == 403, denied.text
    created = client.post("/internal/notifications/alerts", json=body, headers={"X-SignalDesk-Service-Actor": "alert-rule-worker", "X-SignalDesk-Service-Credential": "a" * 32})
    assert created.status_code == 200


def test_alert_derives_authority_and_rejects_forged_tenant_without_mutation(session: Session) -> None:
    request = payload()
    control = FakeControl(authority(request))
    settings = Settings(database_url="postgresql://user:pass@localhost/db", redis_url="redis://localhost/0", alert_rule_worker_service_credential="a" * 32, notification_worker_service_credential="b" * 32, control_api_url="http://control.test", control_api_credential="c" * 32)
    app = create_app(settings=settings, session_factory=sessionmaker(bind=session.get_bind(), expire_on_commit=False), control_client=control)
    headers = {"X-SignalDesk-Service-Actor": "alert-rule-worker", "X-SignalDesk-Service-Credential": "a" * 32}
    forged = request.model_dump(mode="json"); forged["organization_id"] = str(uuid4())
    with TestClient(app) as client:
        response = client.post("/internal/notifications/alerts", json=forged, headers=headers)
    assert response.status_code == 409 and response.json() == {"detail": "notification authority conflict"}
    assert session.scalars(select(Notification)).all() == []


def test_authority_outage_creates_no_notification_or_outbox(session: Session) -> None:
    from signaldesk_notification_api.control_client import ControlAuthorityUnavailable
    request = payload()
    settings = Settings(database_url="postgresql://user:pass@localhost/db", redis_url="redis://localhost/0", alert_rule_worker_service_credential="a" * 32, notification_worker_service_credential="b" * 32, control_api_url="http://control.test", control_api_credential="c" * 32)
    app = create_app(settings=settings, session_factory=sessionmaker(bind=session.get_bind(), expire_on_commit=False), control_client=FakeControl(error=ControlAuthorityUnavailable()))
    headers = {"X-SignalDesk-Service-Actor": "alert-rule-worker", "X-SignalDesk-Service-Credential": "a" * 32}
    with TestClient(app) as client:
        assert client.post("/internal/notifications/alerts", json=request.model_dump(mode="json"), headers=headers).status_code == 503
    assert session.scalars(select(Notification)).all() == [] and session.scalars(select(OutboxEvent)).all() == []


def test_strict_http_scalars_reject_coercion() -> None:
    request = payload().model_dump(mode="json")
    with pytest.raises(ValueError):
        AlertCreate.model_validate({**request, "monitor_id": 1})
    from signaldesk_notification_api.schemas import ClaimRequest, LeaseRequest
    with pytest.raises(ValueError):
        ClaimRequest.model_validate({"lease_seconds": "60"})
    with pytest.raises(ValueError):
        LeaseRequest.model_validate({"lease_token": "x" * 32, "lease_generation": "0"})


def test_readyz_checks_control_and_owned_client_is_closed(session: Session) -> None:
    settings = Settings(database_url="postgresql://user:pass@localhost/db", redis_url="redis://localhost/0", alert_rule_worker_service_credential="a" * 32, notification_worker_service_credential="b" * 32, control_api_url="http://control.test", control_api_credential="c" * 32)
    control = FakeControl(authority(payload()))
    app = create_app(settings=settings, session_factory=sessionmaker(bind=session.get_bind(), expire_on_commit=False), redis_client=FakeRedis(), control_client=control)
    with TestClient(app) as client:
        assert client.get("/readyz").status_code == 200
    assert control.ready_calls == 1 and not control.closed


def test_template_is_closed_and_never_accepts_recipient_data() -> None:
    candidate = payload().model_dump()
    candidate["template_data"]["recipient_email"] = "private@example.test"
    with pytest.raises(ValueError):
        AlertCreate.model_validate(candidate)


def test_migration_has_single_current_head() -> None:
    from alembic.config import Config
    from alembic.script import ScriptDirectory
    script = ScriptDirectory.from_config(Config("alembic.ini"))
    assert script.get_current_head() == "20260825_0001"


class FakeRedis:
    def info(self, section): return {"cluster_enabled": 0}
    def eval(self, *_args): return [1, "1-0"]


def test_outbox_publisher_routes_only_canonical_notification_event(session: Session) -> None:
    create_alert(session, payload()); session.commit()
    assert publish_batch(session=session, redis_client=FakeRedis(), batch_size=1) == 1
    row = session.scalar(select(OutboxEvent))
    assert row is not None and row.published_at is not None
