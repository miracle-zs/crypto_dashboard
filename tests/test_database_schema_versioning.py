import sqlite3

from app.core.database_schema import CURRENT_SCHEMA_VERSION, init_database_schema


class _FakeLogger:
    def info(self, _msg):
        return None

    def warning(self, _msg):
        return None


def _get_user_version(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA user_version")
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else 0


def _get_table_columns(db_path, table_name):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    rows = cur.fetchall()
    conn.close()
    return {str(row[1]) for row in rows}


def test_init_database_schema_sets_user_version_on_new_db(tmp_path):
    db_path = tmp_path / "schema_new.db"
    conn = sqlite3.connect(db_path)

    init_database_schema(conn, _FakeLogger())

    assert _get_user_version(db_path) == CURRENT_SCHEMA_VERSION
    assert "wallet_balance" in _get_table_columns(db_path, "balance_history")
    assert "is_long_term" in _get_table_columns(db_path, "open_positions")


def test_init_database_schema_upgrades_legacy_db_when_user_version_zero(tmp_path):
    db_path = tmp_path / "schema_legacy.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            amount REAL NOT NULL,
            type TEXT DEFAULT 'auto',
            description TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS open_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            symbol TEXT,
            side TEXT,
            entry_time TEXT,
            entry_price REAL,
            qty REAL,
            entry_amount REAL,
            order_id INTEGER,
            UNIQUE(symbol, order_id)
        )
        """
    )
    cur.execute("PRAGMA user_version = 0")
    conn.commit()
    conn.close()

    init_database_schema(sqlite3.connect(db_path), _FakeLogger())

    assert _get_user_version(db_path) == CURRENT_SCHEMA_VERSION
    transfer_columns = _get_table_columns(db_path, "transfers")
    assert {"event_time", "asset", "income_type", "source_uid"}.issubset(transfer_columns)
    open_columns = _get_table_columns(db_path, "open_positions")
    assert {"is_long_term", "profit_alerted", "reentry_alerted"}.issubset(open_columns)
