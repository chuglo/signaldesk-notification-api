from collections.abc import Iterator
from typing import Protocol, TypeAlias

from fastapi import HTTPException, Request, status
from sqlalchemy import create_engine as sqlalchemy_create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

SessionFactory: TypeAlias = sessionmaker[Session]


class DatabaseSettings(Protocol):
    database_url: object


def create_engine(settings: DatabaseSettings) -> Engine:
    url = settings.database_url.unicode_string()
    # Runtime configuration uses the conventional PostgreSQL DSN while this
    # service deliberately ships psycopg3, not the obsolete psycopg2 driver.
    if url.startswith("postgresql://"):
        url = "postgresql+psycopg://" + url.removeprefix("postgresql://")
    return sqlalchemy_create_engine(url, pool_pre_ping=True)


def create_session_factory(engine: Engine) -> SessionFactory:
    return sessionmaker(bind=engine, expire_on_commit=False, close_resets_only=False)


def get_session(request: Request) -> Iterator[Session]:
    factory: SessionFactory | None = request.app.state.session_factory
    if factory is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database unavailable")
    with factory() as session:
        yield session
