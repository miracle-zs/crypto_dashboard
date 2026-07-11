import pandas as pd

from app.core.trade_position_utils import format_holding_time_cn


def merge_same_entry_positions(df: pd.DataFrame) -> pd.DataFrame:
    """Merge positions with same symbol and entry time."""
    if df.empty:
        return df

    local_df = df.copy()
    merged_data = []

    for (symbol, side, entry_order_id), group in local_df.groupby(
        ["Symbol", "Side", "Entry_Order_ID"], dropna=False
    ):
        if len(group) == 1:
            row = group.iloc[0].to_dict()
            merged_data.append(row)
            continue

        total_qty = group["Qty"].sum()
        weighted_entry_price = (group["Entry_Price"] * group["Qty"]).sum() / total_qty
        weighted_exit_price = (group["Exit_Price"] * group["Qty"]).sum() / total_qty

        pnl_net_merged = round(group["PNL_Net"].sum(), 2)
        entry_amount_merged = round(weighted_entry_price * total_qty, 2)
        has_liquidation = any(str(v) == "爆仓" for v in group["Close_Type"].tolist())
        close_type_merged = "爆仓" if has_liquidation else ("止盈" if pnl_net_merged > 0 else "止损")
        return_rate_raw_merged = (pnl_net_merged / entry_amount_merged * 100) if entry_amount_merged != 0 else 0
        return_rate_merged = f"{return_rate_raw_merged:.2f}%"
        entry_time = pd.to_datetime(group["Entry_Time"]).min()
        exit_time = pd.to_datetime(group["Exit_Time"]).max()
        holding_time = format_holding_time_cn((exit_time - entry_time).total_seconds())

        merged_row = {
            "No": group.iloc[0]["No"],
            "Date": group.iloc[0]["Date"],
            "Entry_Time": entry_time.strftime("%Y-%m-%d %H:%M:%S"),
            "Exit_Time": exit_time.strftime("%Y-%m-%d %H:%M:%S"),
            "Holding_Time": holding_time,
            "Symbol": symbol,
            "Side": side,
            "Price_Change_Pct": group.iloc[0]["Price_Change_Pct"],
            "Entry_Amount": entry_amount_merged,
            "Entry_Price": weighted_entry_price,
            "Exit_Price": weighted_exit_price,
            "Qty": total_qty,
            "Fees": round(group["Fees"].sum(), 2),
            "PNL_Net": pnl_net_merged,
            "Close_Type": close_type_merged,
            "Return_Rate": return_rate_merged,
            "Open_Price": group.iloc[0]["Open_Price"],
            "PNL_Before_Fees": round(group["PNL_Before_Fees"].sum(), 2),
            "Entry_Order_ID": entry_order_id,
            "Exit_Order_ID": ",".join(group["Exit_Order_ID"].astype(str).unique()),
        }
        merged_data.append(merged_row)

    merged_df = pd.DataFrame(merged_data)
    merged_df = merged_df.sort_values("Entry_Time", ascending=True).reset_index(drop=True)
    merged_df["No"] = range(1, len(merged_df) + 1)
    return merged_df
