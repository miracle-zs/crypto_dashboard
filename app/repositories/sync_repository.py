class SyncRepository:
    def __init__(self, db):
        self.db = db

    def get_last_entry_time(self):
        return self.db.get_last_entry_time()

    def update_sync_status(self, **kwargs):
        return self.db.update_sync_status(**kwargs)

    def get_symbol_sync_watermarks(self, symbols):
        return self.db.get_symbol_sync_watermarks(symbols)

    def update_symbol_sync_success(self, **kwargs):
        return self.db.update_symbol_sync_success(**kwargs)

    def update_symbol_sync_failure(self, **kwargs):
        return self.db.update_symbol_sync_failure(**kwargs)

    def save_trades(self, df, overwrite: bool = False):
        return self.db.save_trades(df, overwrite=overwrite)

    def recompute_trade_summary(self):
        return self.db.recompute_trade_summary()

    def get_statistics(self):
        return self.db.get_statistics()

    def log_sync_run(self, **kwargs):
        return self.db.log_sync_run(**kwargs)

    def get_open_positions(self):
        return self.db.get_open_positions()
