"""SQLite schema initialization routines."""

from app.core.db_migrations import LATEST_SCHEMA_VERSION, MIGRATIONS

CURRENT_SCHEMA_VERSION = LATEST_SCHEMA_VERSION


def init_database_schema(conn, logger):
    """初始化数据库表结构"""

    # 开启 WAL 模式以支持更高并发
    conn.execute("PRAGMA journal_mode=WAL;")

    cursor = conn.cursor()
    cursor.execute("PRAGMA user_version")
    user_version_row = cursor.fetchone()
    current_version = int(user_version_row[0]) if user_version_row else 0

    if current_version >= CURRENT_SCHEMA_VERSION:
        conn.close()
        return

    for target_version, migrate in MIGRATIONS:
        if target_version <= current_version:
            continue
        migrate(conn, logger)
        cursor.execute(f"PRAGMA user_version = {int(target_version)}")
        current_version = int(target_version)

    conn.commit()
    conn.close()
