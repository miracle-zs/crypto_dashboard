import sqlite3

from app.database import Database


def test_open_positions_hot_query_uses_index(tmp_path):
    db = Database(db_path=str(tmp_path / "hotpath.db"))
    conn = sqlite3.connect(db.db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("PRAGMA index_list('open_positions')")
    index_names = {row[1] for row in cur.fetchall()}
    assert "idx_open_positions_entry_time" in index_names
    assert "idx_open_positions_profit_alerted_entry" in index_names
    assert "idx_open_positions_alerted_last_alert" in index_names

    cur.execute("EXPLAIN QUERY PLAN SELECT * FROM open_positions ORDER BY entry_time DESC")
    plan_rows = cur.fetchall()
    details = " ".join(str(row[3]).upper() for row in plan_rows)
    assert "INDEX" in details

    conn.close()
