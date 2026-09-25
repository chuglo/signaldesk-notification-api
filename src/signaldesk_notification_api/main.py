from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
from redis import Redis
from signaldesk_service_kit import build_service_auth_dependency
from sqlalchemy import text

from .control_client import ControlClient, HttpControlClient
from .database import SessionFactory, create_engine, create_session_factory
from .routes import _auth, router
from .settings import Settings


def create_app(*, settings: Settings | None = None, session_factory: SessionFactory | None = None, redis_client: Redis | None = None, control_client: ControlClient | None = None) -> FastAPI:
    engine = None
    if settings is not None and session_factory is None:
        engine = create_engine(settings); session_factory = create_session_factory(engine)
    owns_control_client = settings is not None and control_client is None
    if owns_control_client:
        control_client = HttpControlClient(base_url=str(settings.control_api_url), credential=settings.control_api_credential.get_secret_value())
    auth = None if settings is None else build_service_auth_dependency(credentials=settings.credentials(), audience="signaldesk-notification-api", allowed_actors={"alert-rule-worker", "notification-worker"})
    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try: yield
        finally:
            if engine is not None: engine.dispose()
            if owns_control_client and control_client is not None: control_client.close()
    app = FastAPI(lifespan=lifespan)
    app.state.settings = settings; app.state.session_factory = session_factory; app.state.redis_client = redis_client; app.state.control_client = control_client
    if auth is not None: app.dependency_overrides[_auth] = lambda: auth.dependency  # replaced below by direct route dependency resolution
    # Route-level callable is overridden with an actual dependency callable, preserving test injection.
    if auth is not None: app.dependency_overrides[_auth] = auth.dependency
    app.include_router(router)
    @app.get("/healthz")
    def healthz() -> dict[str, str]: return {"status": "ok"}
    @app.get("/readyz")
    def readyz():
        created_client = False
        try:
            if session_factory is None or settings is None or control_client is None: raise RuntimeError("unconfigured")
            with session_factory() as session: session.execute(text("SELECT 1"))
            client = redis_client or Redis.from_url(settings.redis_url.unicode_string(), socket_connect_timeout=2, socket_timeout=2)
            created_client = redis_client is None
            info = client.info("cluster")
            if not isinstance(info, dict) or info.get("cluster_enabled") not in (0, "0", False): raise RuntimeError("redis topology")
            control_client.ready()
            return {"status": "ok"}
        except Exception:
            return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"detail": "dependencies unavailable"})
        finally:
            if created_client:
                client.close()
    return app


def create_configured_app() -> FastAPI:
    return create_app(settings=Settings())


app = create_app()
