from app.repositories.sync_read_repository import SyncReadRepository
from app.repositories.sync_write_repository import SyncWriteRepository


class SyncRepository:
    """Backward-compatible facade for sync read/write repositories."""

    def __init__(self, db):
        self.db = db
        self._read = SyncReadRepository(db) if db is not None else None
        self._write = SyncWriteRepository(db) if db is not None else None
        self._open_positions_state_columns = None

    def get_last_entry_time(self):
        return self._read.get_last_entry_time()

    def update_sync_status(self, **kwargs):
        status = kwargs.get("status", "idle")
        error_message = kwargs.get("error_message")
        last_entry_time = self.get_last_entry_time()

        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM trades")
        total_trades = int(cursor.fetchone()[0] or 0)
        conn.close()

        return self._write.update_sync_status(
            status=status,
            error_message=error_message,
            last_entry_time=last_entry_time,
            total_trades=total_trades,
        )

    def get_symbol_sync_watermarks(self, symbols):
        return self._read.get_symbol_sync_watermarks(symbols)

    def update_symbol_sync_success(self, **kwargs):
        symbol = kwargs.get("symbol")
        end_ms = kwargs.get("end_ms")
        if not symbol or end_ms is None:
            return 0
        return self.update_symbol_sync_success_batch([symbol], end_ms=int(end_ms))

    def update_symbol_sync_failure(self, **kwargs):
        symbol = kwargs.get("symbol")
        end_ms = kwargs.get("end_ms")
        error_message = kwargs.get("error_message")
        if not symbol or end_ms is None:
            return 0
        return self.update_symbol_sync_failure_batch(
            failures={str(symbol).upper(): error_message},
            end_ms=int(end_ms),
        )

    def update_symbol_sync_success_batch(self, symbols, end_ms: int):
        return self._write.update_symbol_sync_success_batch(symbols, end_ms)

    def update_symbol_sync_failure_batch(self, failures, end_ms: int):
        return self._write.update_symbol_sync_failure_batch(failures, end_ms)

    def save_trades(self, df, overwrite: bool = False):
        return self._write.save_trades(df, overwrite)

    def recompute_trade_summary(self):
        return self._read.recompute_trade_summary()

    def get_statistics(self):
        return self._read.get_statistics()

    def log_sync_run(self, **kwargs):
        return self._write.log_sync_run(**kwargs)

    def get_sync_status(self):
        return self._read.get_sync_status()

    def list_sync_run_logs(self, limit: int = 100):
        return self._read.list_sync_run_logs(limit=limit)

    def get_open_positions(self):
        return self._read.get_open_positions()

    def get_open_position_symbols(self):
        return self._read.get_open_position_symbols()

    def save_open_positions(self, rows):
        saved = self._write.save_open_positions(rows)
        self._open_positions_state_columns = self._write._open_positions_state_columns
        return saved

    def get_latest_transfer_event_time(self):
        return self._read.get_latest_transfer_event_time()

    def save_transfer_income(self, **kwargs):
        return self._write.save_transfer_income(**kwargs)
