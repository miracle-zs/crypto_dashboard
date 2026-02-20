class RiskRepository:
    def __init__(self, db):
        self.db = db

    def get_open_positions(self):
        return self.db.get_open_positions()

    def save_noon_loss_snapshot(self, **kwargs):
        return self.db.save_noon_loss_snapshot(**kwargs)

    def get_noon_loss_snapshot_by_date(self, snapshot_date: str):
        return self.db.get_noon_loss_snapshot_by_date(snapshot_date)

    def save_noon_loss_review_snapshot(self, **kwargs):
        return self.db.save_noon_loss_review_snapshot(**kwargs)
