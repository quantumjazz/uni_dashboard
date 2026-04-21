import sqlite3
from contextlib import contextmanager
from pathlib import Path

from backend.app.config import get_settings


def _sqlite_path() -> Path:
    settings = get_settings()
    return Path(settings.database_url.replace("sqlite:///", ""))


def _create_data_points_table(conn: sqlite3.Connection, table_name: str = "data_points") -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            source TEXT NOT NULL,
            dataset TEXT NOT NULL,
            indicator TEXT NOT NULL,
            country TEXT NOT NULL,
            year INTEGER NOT NULL,
            series_key TEXT NOT NULL DEFAULT '',
            series_label TEXT,
            value REAL NOT NULL,
            unit TEXT,
            note TEXT,
            dimensions_json TEXT NOT NULL DEFAULT '{{}}',
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (indicator, country, year, series_key)
        )
        """
    )


def _data_points_schema_is_current(conn: sqlite3.Connection) -> bool:
    columns = conn.execute("PRAGMA table_info(data_points)").fetchall()
    if not columns:
        return False

    column_names = {column[1] for column in columns}
    required = {"series_key", "series_label", "dimensions_json"}
    if not required.issubset(column_names):
        return False

    pk_columns = [column[1] for column in sorted(columns, key=lambda column: column[5]) if column[5] > 0]
    return pk_columns == ["indicator", "country", "year", "series_key"]


def _ensure_data_points_schema(conn: sqlite3.Connection) -> None:
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'data_points'"
    ).fetchone()
    if not exists:
        _create_data_points_table(conn)
        return

    if _data_points_schema_is_current(conn):
        return

    _create_data_points_table(conn, "data_points_v2")
    conn.execute(
        """
        INSERT INTO data_points_v2 (
            source,
            dataset,
            indicator,
            country,
            year,
            series_key,
            series_label,
            value,
            unit,
            note,
            dimensions_json,
            fetched_at
        )
        SELECT
            source,
            dataset,
            indicator,
            country,
            year,
            '',
            NULL,
            value,
            unit,
            note,
            '{}',
            fetched_at
        FROM data_points
        """
    )
    conn.execute("DROP TABLE data_points")
    conn.execute("ALTER TABLE data_points_v2 RENAME TO data_points")


def _ensure_institution_crosswalk_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS institutions (
            institution_uid   TEXT PRIMARY KEY,
            canonical_name    TEXT NOT NULL,
            country_code      TEXT NOT NULL,
            website_host      TEXT,
            eter_id           TEXT UNIQUE,
            institution_type  TEXT,
            legal_status      TEXT,
            status            TEXT NOT NULL DEFAULT 'active',
            merged_into_uid   TEXT REFERENCES institutions(institution_uid),
            first_seen_at     TEXT NOT NULL,
            last_verified_at  TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_institutions_country ON institutions(country_code)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_institutions_website_host ON institutions(website_host)"
    )
    institution_columns = {column[1] for column in conn.execute("PRAGMA table_info(institutions)").fetchall()}
    if "institution_type" not in institution_columns:
        conn.execute("ALTER TABLE institutions ADD COLUMN institution_type TEXT")
    if "legal_status" not in institution_columns:
        conn.execute("ALTER TABLE institutions ADD COLUMN legal_status TEXT")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS institution_identifiers (
            scheme           TEXT NOT NULL,
            value            TEXT NOT NULL,
            institution_uid  TEXT NOT NULL REFERENCES institutions(institution_uid),
            source           TEXT NOT NULL,
            confidence       REAL NOT NULL,
            asserted_at      TEXT NOT NULL,
            PRIMARY KEY (scheme, value)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_institution_identifiers_uid "
        "ON institution_identifiers(institution_uid)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS institution_name_variants (
            institution_uid  TEXT NOT NULL REFERENCES institutions(institution_uid),
            normalized       TEXT NOT NULL,
            variant          TEXT NOT NULL,
            language         TEXT,
            source           TEXT NOT NULL,
            PRIMARY KEY (institution_uid, normalized)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_institution_name_variants_normalized "
        "ON institution_name_variants(normalized)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS institution_match_log (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            institution_uid  TEXT,
            input_scheme     TEXT NOT NULL,
            input_value      TEXT NOT NULL,
            match_type       TEXT NOT NULL,
            confidence       REAL NOT NULL,
            resolved_at      TEXT NOT NULL
        )
        """
    )


def init_db() -> None:
    db_path = _sqlite_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        _ensure_data_points_schema(conn)
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
        _ensure_institution_crosswalk_schema(conn)
        conn.commit()


@contextmanager
def get_connection():
    conn = sqlite3.connect(_sqlite_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()
