class SettingsRepository:
    def __init__(self, db):
        self.db = db

    def set_position_long_term(self, symbol: str, order_id: int, is_long_term: bool):
        return self.db.set_position_long_term(symbol, order_id, is_long_term)
