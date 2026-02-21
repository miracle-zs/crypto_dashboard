def fetch_open_positions(db):
    conn = db._get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM open_positions ORDER BY entry_time DESC")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def fetch_open_position_symbols(db):
    conn = db._get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT DISTINCT symbol
        FROM open_positions
        WHERE symbol IS NOT NULL AND symbol != ''
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return [str(row["symbol"]) for row in rows]
