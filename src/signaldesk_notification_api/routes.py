from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session
from signaldesk_service_kit import ServicePrincipal
from uuid import UUID

from .database import get_session
from .control_client import ControlAuthorityUnavailable, ControlClient
from .schemas import AlertCreate, AttachRequest, ClaimRequest, ClaimResponse, FailRequest, NotificationResponse, TerminalAuthority
from .services import attach, claim, create_alert, mark_failed

router = APIRouter(prefix="/internal/notifications", tags=["notifications"])


def _auth(*_args, **_kwargs):
    raise RuntimeError("authentication was not configured")


def _actor(principal: ServicePrincipal, expected: str) -> None:
    if principal.actor != expected:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="service actor not allowed")


def get_control_client(request: Request) -> ControlClient:
    client: ControlClient | None = request.app.state.control_client
    if client is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="authority unavailable")
    return client


def _matches_authority(request: AlertCreate, authority: TerminalAuthority) -> bool:
    return (
        request.diagnostic_job_id == authority.diagnostic_job_id
        and request.organization_id == authority.organization_id
        and request.correlation_id == authority.correlation_id
        and request.requested_by_user_id == authority.requested_by_user_id
        and request.template_data.status == authority.status
        and request.template_data.outcome == authority.outcome
        and request.template_data.error_code == authority.error_code
    )


def _authority_request(request: AlertCreate, authority: TerminalAuthority) -> AlertCreate:
    """Persist terminal tenant facts only after control-api comparison succeeds."""
    return request.model_copy(update={
        "organization_id": authority.organization_id,
        "correlation_id": authority.correlation_id,
        "requested_by_user_id": authority.requested_by_user_id,
        "diagnostic_job_id": authority.diagnostic_job_id,
        "template_data": request.template_data.model_copy(update={
            "diagnostic_job_id": authority.diagnostic_job_id,
            "status": authority.status,
            "outcome": authority.outcome,
            "error_code": authority.error_code,
        }),
    })


@router.post("/alerts", response_model=NotificationResponse)
def alert(request: AlertCreate, session: Session = Depends(get_session), control_client: ControlClient = Depends(get_control_client), _principal: ServicePrincipal = Depends(_auth)):
    _actor(_principal, "alert-rule-worker")
    try:
        authority = control_client.terminal_authority(request.diagnostic_job_id)
    except ControlAuthorityUnavailable:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="authority unavailable") from None
    if not _matches_authority(request, authority):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="notification authority conflict")
    result = create_alert(session, _authority_request(request, authority)); session.commit(); return result

@router.post("/{notification_id}/claim", response_model=ClaimResponse | NotificationResponse)
def claim_notification(notification_id: UUID, request: ClaimRequest, session: Session = Depends(get_session), _principal: ServicePrincipal = Depends(_auth)):
    _actor(_principal, "notification-worker")
    result = claim(session, notification_id, request.lease_seconds); session.commit(); return result

@router.post("/{notification_id}/email", response_model=NotificationResponse)
def attach_email(notification_id: UUID, request: AttachRequest, session: Session = Depends(get_session), _principal: ServicePrincipal = Depends(_auth)):
    _actor(_principal, "notification-worker")
    result = attach(session, notification_id, request.lease_token, request.lease_generation, request.email_delivery_id); session.commit(); return result

@router.post("/{notification_id}/failed", response_model=NotificationResponse)
def failed(notification_id: UUID, request: FailRequest, session: Session = Depends(get_session), _principal: ServicePrincipal = Depends(_auth)):
    _actor(_principal, "notification-worker")
    result = mark_failed(session, notification_id, request.lease_token, request.lease_generation, request.failure_code); session.commit(); return result
