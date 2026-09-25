from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    # JSON has no UUID scalar; field shapes remain closed while JSON UUIDs are
    # parsed at the HTTP boundary.
    model_config = ConfigDict(extra="forbid", strict=True)


# Pydantic's strict mode intentionally does not coerce Python objects.  HTTP JSON
# has no UUID or datetime scalar, so these are the only boundary exceptions.
JsonUUID = Annotated[UUID, Field(strict=False)]
JsonDatetime = Annotated[datetime, Field(strict=False)]


def _canonical_uuid(value: Any) -> Any:
    if isinstance(value, str) and str(UUID(value)) != value:
        raise ValueError("UUID must use canonical lowercase form")
    return value


class TemplateData(StrictModel):
    monitor_id: JsonUUID
    monitor_run_id: JsonUUID
    diagnostic_job_id: JsonUUID
    status: Literal["completed", "failed"]
    outcome: Literal["reachable", "error", "blocked"] | None = Field(...)
    error_code: str | None = Field(..., min_length=1, max_length=64, pattern=r"^[a-z0-9]+(?:_[a-z0-9]+)*$")

    _canonical_ids = field_validator("monitor_id", "monitor_run_id", "diagnostic_job_id", mode="before")(_canonical_uuid)

    @model_validator(mode="after")
    def coherent(self):
        if self.status == "completed" and self.outcome not in ("error", "blocked"):
            raise ValueError("completed alerts require error or blocked outcome")
        return self


class AlertCreate(StrictModel):
    organization_id: JsonUUID
    correlation_id: JsonUUID
    monitor_id: JsonUUID
    monitor_run_id: JsonUUID
    diagnostic_job_id: JsonUUID
    requested_by_user_id: JsonUUID
    template_name: Literal["diagnostic_alert"]
    template_data: TemplateData

    _canonical_ids = field_validator("organization_id", "correlation_id", "monitor_id", "monitor_run_id", "diagnostic_job_id", "requested_by_user_id", mode="before")(_canonical_uuid)

    @field_validator("template_data")
    @classmethod
    def matching_ids(cls, data: TemplateData, info):
        values = info.data
        for field in ("monitor_id", "monitor_run_id", "diagnostic_job_id"):
            if field in values and getattr(data, field) != values[field]:
                raise ValueError("template identifiers must match request")
        return data


class ClaimRequest(StrictModel):
    lease_seconds: int = Field(default=60, ge=5, le=300)


class LeaseRequest(StrictModel):
    lease_token: str = Field(min_length=32, max_length=256)
    lease_generation: int = Field(ge=0)


class AttachRequest(LeaseRequest):
    email_delivery_id: JsonUUID

    _canonical_id = field_validator("email_delivery_id", mode="before")(_canonical_uuid)


class FailRequest(LeaseRequest):
    failure_code: Literal["recipient_unavailable", "invalid_template", "delivery_failed", "member_removed"]


class NotificationResponse(StrictModel):
    id: JsonUUID
    organization_id: JsonUUID
    correlation_id: JsonUUID
    state: Literal["pending", "claimed", "email_attached", "failed"]
    monitor_id: JsonUUID
    monitor_run_id: JsonUUID
    diagnostic_job_id: JsonUUID
    requested_by_user_id: JsonUUID
    template_name: Literal["diagnostic_alert"]
    template_data: TemplateData
    email_delivery_id: JsonUUID | None
    failure_code: str | None
    lease_generation: int
    lease_expires_at: JsonDatetime | None


class TerminalAuthority(StrictModel):
    """Frozen control-api contract for a terminal diagnostic job."""

    diagnostic_job_id: JsonUUID
    organization_id: JsonUUID
    requested_by_user_id: JsonUUID
    correlation_id: JsonUUID
    status: Literal["completed", "failed"]
    outcome: Literal["reachable", "error", "blocked"] | None = Field(...)
    error_code: str | None = Field(..., min_length=1, max_length=64, pattern=r"^[a-z0-9]+(?:_[a-z0-9]+)*$")
    notification_mode: Literal["alert_rule"]

    _canonical_ids = field_validator("diagnostic_job_id", "organization_id", "requested_by_user_id", "correlation_id", mode="before")(_canonical_uuid)


class ClaimResponse(NotificationResponse):
    lease_token: str
