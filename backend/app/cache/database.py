import sqlite3
from contextlib import contextmanager
from pathlib import Path

from backend.app.config import get_settings


def _sqlite_path() -> Path:
    settings = get_settings()
    return Path(settings.database_url.replace("sqlite:///", ""))


def init_db() -> None:
    db_path = _sqlite_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS data_points (
                source TEXT NOT NULL,
                dataset TEXT NOT NULL,
                indicator TEXT NOT NULL,
                country TEXT NOT NULL,
                year INTEGER NOT NULL,
                value REAL NOT NULL,
                unit TEXT,
                note TEXT,
                fetched_at TEXT NOT NULL,
                PRIMARY KEY (indicator, country, year)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS api_cache (
                cache_key TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata_cache (
                cache_key TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


@contextmanager
def get_connection():
    conn = sqlite3.connect(_sqlite_path())
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()
