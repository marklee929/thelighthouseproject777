from __future__ import annotations

import importlib
import os
from dataclasses import dataclass


@dataclass(slots=True)
class PostgresConnectionFactory:
    """Lazily loads psycopg so imports stay cheap until a DB connection is needed."""

    dsn: str

    @classmethod
    def from_env(cls) -> "PostgresConnectionFactory":
        default_dsn = "postgresql://postgres:postgres@localhost:5432/lighthouse"
        dsn = (
            os.getenv("LIGHTHOUSE_DATABASE_DSN", "").strip()
            or os.getenv("DATABASE_URL", "").strip()
            or default_dsn
        )
        return cls(dsn=dsn)

    def connect(self):
        try:
            psycopg = importlib.import_module("psycopg")
            rows = importlib.import_module("psycopg.rows")
        except ImportError as exc:
            raise RuntimeError("psycopg is required for PostgreSQL repositories. Install psycopg[binary].") from exc
        return psycopg.connect(self.dsn, row_factory=rows.dict_row)
