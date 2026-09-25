import secrets
from typing import Self

from pydantic import AnyHttpUrl, PostgresDsn, RedisDsn, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from signaldesk_service_kit import ServiceCredential, ServiceCredentialSet, ServicePrincipal


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SIGNALDESK_NOTIFICATION_", extra="forbid", strict=True)
    database_url: PostgresDsn
    redis_url: RedisDsn
    alert_rule_worker_service_credential: SecretStr
    notification_worker_service_credential: SecretStr
    control_api_url: AnyHttpUrl
    control_api_credential: SecretStr
    service_name: str = "signaldesk-notification-api"

    @field_validator("alert_rule_worker_service_credential", "notification_worker_service_credential", "control_api_credential")
    @classmethod
    def strong(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        if len(raw) < 32 or raw != raw.strip() or not raw.isascii():
            raise ValueError("service credentials must be at least 32 ASCII characters without surrounding whitespace")
        return value

    @model_validator(mode="after")
    def distinct(self) -> Self:
        credentials = (self.alert_rule_worker_service_credential, self.notification_worker_service_credential, self.control_api_credential)
        raw = [credential.get_secret_value() for credential in credentials]
        if len(set(raw)) != len(raw):
            raise ValueError("service credentials must be distinct")
        return self

    def credentials(self) -> ServiceCredentialSet:
        return ServiceCredentialSet(credentials=(
            ServiceCredential(principal=ServicePrincipal(actor="alert-rule-worker", audience="signaldesk-notification-api"), credential=self.alert_rule_worker_service_credential),
            ServiceCredential(principal=ServicePrincipal(actor="notification-worker", audience="signaldesk-notification-api"), credential=self.notification_worker_service_credential),
        ))


class OutboxPublisherSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SIGNALDESK_NOTIFICATION_", extra="forbid", strict=True)
    database_url: PostgresDsn
    redis_url: RedisDsn
    service_name: str = "signaldesk-notification-outbox-publisher"
