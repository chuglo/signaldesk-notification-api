# SignalDesk Notification API

**Not for production use.**

Internal, tenant-scoped notification authority. It stores identifiers and one finite
`diagnostic_alert` template only; it never stores a recipient or target/body data and
does not send SMTP. The email worker owns recipient selection and SMTP delivery.
`email_attached` is this service's successful terminal orchestration state; the
separate delivery `sent`/`failed` authority remains with control-api and the
email worker and is verified in their end-to-end flow.

Routes (all service-authenticated): `POST /internal/notifications/alerts` is for
`alert-rule-worker`; it first obtains terminal authority from control-api as
`notification-api` at `GET /internal/notification-api/diagnostics/{diagnostic_job_id}/terminal`.
The response must be the closed terminal contract (including `notification_mode: "alert_rule"`),
and submitted tenant, requester, correlation, and terminal facts must match. The database only
receives the control-derived values. Control absence, non-terminal/manual responses, malformed
responses, and outages fail closed and create neither notification nor outbox event. `POST /internal/notifications/{id}/claim`, `/email`, and
`/failed` are for `notification-worker`. The states are `pending`, `claimed`,
`email_attached`, and `failed`. `email_attached` is terminal orchestration success:
it proves exactly one authoritative control-email delivery was attached, not that an
SMTP message was sent. There is intentionally no `/sent` route.

A successful active claim alone returns a raw lease token, generation, and expiry.
Attach/fail require both token and generation. Exact attach/failure retries return
the terminal result without a token; a different delivery/failure conflicts.

The publisher emits only `notification.requested.v1` to `signaldesk:notifications`
using the two-field streams-kit envelope (`event`, `event_id`). It requires standalone
Redis, uses bounded batches (1–100), and polls at a bounded interval (0.1–60 seconds).
Before an append it retains a bounded tail only when every existing consumer group is
caught up (zero pending and lag); it never trims no-group, pending, unread, or
unknown-lag streams. The production defaults are `--stream-maxlen 10000` and
`--replay-marker-ttl-seconds 604800` (seven days).
Use `signaldesk-notification-outbox-publisher --once` for a single batch, or pass
`--batch-size` and `--poll-interval` for daemon mode.

Container builds require named relative BuildKit contexts: `contracts=../signaldesk-contracts`
and `service-kit=../signaldesk-service-kit`; the Dockerfile installs the locked,
no-dev, non-editable environment as UID 10001.

Run migrations with `alembic upgrade head`; run the bounded publisher with
`signaldesk-notification-outbox-publisher --once`.

Runtime settings additionally require `SIGNALDESK_NOTIFICATION_CONTROL_API_URL` and
`SIGNALDESK_NOTIFICATION_CONTROL_API_CREDENTIAL`. The control credential is mandatory and must
be distinct from both inbound worker credentials. Readiness checks Postgres, standalone Redis,
and control-api `/readyz`.

## License

MIT. See [LICENSE](LICENSE).
