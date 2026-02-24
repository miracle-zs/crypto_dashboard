import os
from datetime import datetime, timedelta, timezone

from app.logger import logger


def run_balance_sync_job(scheduler) -> str:
    if not scheduler.processor:
        return "success"
    if scheduler._is_api_cooldown_active(source="余额同步"):
        return "success"
    if not scheduler._try_enter_api_job_slot(source="余额同步"):
        return "success"

    status = "success"
    try:
        logger.info("开始同步账户余额...")
        balance_info = scheduler.processor.get_account_balance()

        if balance_info:
            current_margin = balance_info["margin_balance"]
            current_wallet = balance_info["wallet_balance"]

            try:
                latest_event_time_ms = scheduler.sync_repo.get_latest_transfer_event_time()
                if latest_event_time_ms is None:
                    lookback_days_raw = os.getenv("TRANSFER_SYNC_LOOKBACK_DAYS", "90")
                    try:
                        lookback_days = max(1, int(lookback_days_raw))
                    except ValueError:
                        logger.warning(f"TRANSFER_SYNC_LOOKBACK_DAYS={lookback_days_raw} 非法，回退为 90")
                        lookback_days = 90
                    start_time_ms = int((datetime.now(timezone.utc) - timedelta(days=lookback_days)).timestamp() * 1000)
                else:
                    start_time_ms = max(0, latest_event_time_ms - 60_000)

                end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                transfer_rows = scheduler.processor.get_transfer_income_records(
                    start_time=start_time_ms,
                    end_time=end_time_ms,
                )

                inserted_count = 0
                for row in transfer_rows:
                    inserted = scheduler.sync_repo.save_transfer_income(
                        amount=row["amount"],
                        event_time=row["event_time_ms"],
                        asset=row.get("asset") or "USDT",
                        income_type=row.get("income_type") or "TRANSFER",
                        source_uid=row.get("source_uid"),
                        description=row.get("description"),
                    )
                    if inserted:
                        inserted_count += 1

                logger.info(
                    "出入金同步完成: "
                    f"fetched={len(transfer_rows)}, inserted={inserted_count}, "
                    f"window=[{scheduler._format_ms_to_utc8(start_time_ms)} ~ {scheduler._format_ms_to_utc8(end_time_ms)}]"
                )
            except Exception as exc:
                logger.warning(f"出入金同步出错: {exc}")

            scheduler.trade_repo.save_balance_history(current_margin, current_wallet)
            logger.info(f"余额已更新: {current_margin:.2f} USDT (Wallet: {current_wallet:.2f})")
        else:
            logger.warning("获取余额失败，balance为 None")
    except Exception as exc:
        status = "error"
        logger.error(f"同步余额失败: {str(exc)}")
    finally:
        scheduler._release_api_job_slot()

    return status
