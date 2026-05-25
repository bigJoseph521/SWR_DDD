from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, cast

from sqlalchemy import event
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine import create_engine as sqlalchemy_create_engine


def create_engine(db_path: Path) -> Engine:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = sqlalchemy_create_engine(
        f"sqlite+pysqlite:///{db_path.as_posix()}",
        future=True,
        connect_args={"timeout": 30},
    )

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection: object, connection_record: object) -> None:
        cursor = cast(Any, dbapi_connection).cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
        finally:
            cursor.close()

    return engine


@contextmanager
def begin_connection(engine: Engine) -> Iterator[Connection]:
    with engine.begin() as connection:
        yield connection
