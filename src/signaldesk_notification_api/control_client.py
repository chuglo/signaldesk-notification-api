"""Fail-closed client for control-api terminal diagnostic authority."""
from typing import Protocol
from uuid import UUID

import httpx

from .schemas import TerminalAuthority


class ControlAuthorityUnavailable(RuntimeError):
    pass


class ControlClient(Protocol):
    def terminal_authority(self, diagnostic_job_id: UUID) -> TerminalAuthority: ...
    def ready(self) -> None: ...
    def close(self) -> None: ...


class HttpControlClient:
    def __init__(self, *, base_url: str, credential: str) -> None:
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"), timeout=httpx.Timeout(2.0, connect=1.0),
            limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
            headers={"X-SignalDesk-Service-Actor": "notification-api", "X-SignalDesk-Service-Credential": credential},
        )

    def terminal_authority(self, diagnostic_job_id: UUID) -> TerminalAuthority:
        try:
            response = self._client.get(f"/internal/notification-api/diagnostics/{diagnostic_job_id}/terminal")
            response.raise_for_status()
            if len(response.content) > 16 * 1024:
                raise ValueError("control authority response too large")
            return TerminalAuthority.model_validate(response.json())
        except (httpx.HTTPError, ValueError) as error:
            raise ControlAuthorityUnavailable("control authority unavailable") from error

    def ready(self) -> None:
        try:
            response = self._client.get("/readyz")
            response.raise_for_status()
        except httpx.HTTPError as error:
            raise ControlAuthorityUnavailable("control authority unavailable") from error

    def close(self) -> None:
        self._client.close()
