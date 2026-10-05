from app.core.read_model import get_read_model
from app.repositories.sync_read_repository import SyncReadRepository
from app.repositories.sync_write_repository import SyncWriteRepository


class SyncRepository(SyncReadRepository, SyncWriteRepository):
    """The public persistence interface for synchronization state and facts."""

    def __init__(self, db):
        SyncReadRepository.__init__(self, db)
        SyncWriteRepository.__init__(self, db)

    def update_sync_status(self, **kwargs):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM trades")
        total_trades = int(cursor.fetchone()[0] or 0)
        conn.close()
        return SyncWriteRepository.update_sync_status(
            self,
            status=kwargs.get("status", "idle"),
            error_message=kwargs.get("error_message"),
            last_entry_time=self.get_last_entry_time(),
            total_trades=total_trades,
        )

    def update_symbol_sync_success(self, **kwargs):
        symbol = kwargs.get("symbol")
        end_ms = kwargs.get("end_ms")
        if not symbol or end_ms is None:
            return 0
        return self.update_symbol_sync_success_batch([symbol], end_ms=int(end_ms))

    def update_symbol_sync_failure(self, **kwargs):
        symbol = kwargs.get("symbol")
        end_ms = kwargs.get("end_ms")
        if not symbol or end_ms is None:
            return 0
        return self.update_symbol_sync_failure_batch(
            failures={str(symbol).upper(): kwargs.get("error_message")},
            end_ms=int(end_ms),
        )

    def save_open_positions(self, rows):
        saved = SyncWriteRepository.save_open_positions(self, rows)
        get_read_model().invalidate(prefix="positions:open:")
        return saved

    def save_position_snapshots(self, snapshot_time: str, positions: list) -> int:
        saved = SyncWriteRepository.save_position_snapshots(self, snapshot_time, positions)
        get_read_model().invalidate(prefix="positions:snapshots:")
        return saved
