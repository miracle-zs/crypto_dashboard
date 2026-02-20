class TradeRepository:
    def __init__(self, db):
        self.db = db

    def get_trade_summary(self):
        return self.db.get_trade_summary()

    def get_statistics(self):
        return self.db.get_statistics()

    def recompute_trade_summary(self):
        return self.db.recompute_trade_summary()

    def get_all_trades(self):
        return self.db.get_all_trades()

    def get_open_positions(self):
        return self.db.get_open_positions()

    def get_balance_history(self, **kwargs):
        return self.db.get_balance_history(**kwargs)

    def get_transfers(self):
        return self.db.get_transfers()
