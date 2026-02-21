from datetime import datetime, timedelta, timezone

from app.core.async_utils import run_in_thread
from app.repositories import TradeRepository


class BalanceService:
    async def build_balance_history_response(self, *, db, time_range: str):
        trade_repo = TradeRepository(db)
        end_time = datetime.utcnow()
        start_time = None

        if time_range == "1h":
            start_time = end_time - timedelta(hours=1)
        elif time_range == "1d":
            start_time = end_time - timedelta(days=1)
        elif time_range == "1w":
            start_time = end_time - timedelta(weeks=1)
        elif time_range == "1m":
            start_time = end_time - timedelta(days=30)
        elif time_range == "1y":
            start_time = end_time - timedelta(days=365)
        else:
            start_time = end_time - timedelta(hours=2)

        history_data = await run_in_thread(
            trade_repo.get_balance_history, start_time=start_time, end_time=end_time
        )
        if not history_data:
            return []

        transfers = await run_in_thread(trade_repo.get_transfers)

        sorted_transfers = []
        for t in transfers:
            try:
                t_dt = datetime.fromisoformat(t["timestamp"]).replace(tzinfo=timezone.utc)
                sorted_transfers.append((t_dt, float(t["amount"])))
            except (TypeError, ValueError):
                continue
        sorted_transfers.sort(key=lambda x: x[0])

        transformed_data = []
        transfer_idx = 0
        current_net_deposits = 0.0
        total_transfers = len(sorted_transfers)

        first_utc_dt = datetime.fromisoformat(history_data[0]["timestamp"]).replace(tzinfo=timezone.utc)
        while transfer_idx < total_transfers and sorted_transfers[transfer_idx][0] <= first_utc_dt:
            current_net_deposits += sorted_transfers[transfer_idx][1]
            transfer_idx += 1
        baseline_net_deposits = current_net_deposits

        total_points = len(history_data)
        target_points = 1000
        step = 1
        if total_points > target_points:
            step = total_points // target_points

        for i, item in enumerate(history_data):
            if i % step != 0 and i != total_points - 1:
                continue

            utc_dt_naive = datetime.fromisoformat(item["timestamp"])
            utc_dt_aware = utc_dt_naive.replace(tzinfo=timezone.utc)
            current_ts = int(utc_dt_aware.timestamp() * 1000)
            point_transfer_amount = 0.0
            point_transfer_count = 0

            while transfer_idx < total_transfers and sorted_transfers[transfer_idx][0] <= utc_dt_aware:
                transfer_amount = sorted_transfers[transfer_idx][1]
                current_net_deposits += transfer_amount
                point_transfer_amount += transfer_amount
                point_transfer_count += 1
                transfer_idx += 1

            net_transfer_in_range = current_net_deposits - baseline_net_deposits
            cumulative_val = item["balance"] - net_transfer_in_range

            transformed_data.append(
                {
                    "time": current_ts,
                    "value": item["balance"],
                    "cumulative_equity": cumulative_val,
                    "transfer_amount": point_transfer_amount if point_transfer_count > 0 else None,
                    "transfer_count": point_transfer_count if point_transfer_count > 0 else None,
                }
            )

        return transformed_data
