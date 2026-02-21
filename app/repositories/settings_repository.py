class SettingsRepository:
    def __init__(self, db):
        self.db = db

    def set_position_long_term(self, symbol: str, order_id: int, is_long_term: bool):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE open_positions
            SET is_long_term = ?
            WHERE symbol = ? AND order_id = ?
            """,
            (1 if is_long_term else 0, str(symbol), int(order_id)),
        )
        conn.commit()
        conn.close()
        return None
