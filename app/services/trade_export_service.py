from datetime import datetime

import pandas as pd


def export_to_excel_file(df: pd.DataFrame, filename: str | None = None) -> str:
    """Export trade dataframe to an xlsx file and return output path."""
    if filename is None:
        filename = f"order_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Orders", index=False)

        worksheet = writer.sheets["Orders"]

        if "Price_Change_Pct" in df.columns:
            col_idx = df.columns.get_loc("Price_Change_Pct")
            col_letter = chr(ord("A") + col_idx)
            for row in range(2, len(df) + 2):
                cell = worksheet[f"{col_letter}{row}"]
                cell.number_format = "0%"

        last_row = len(df) + 2
        worksheet[f"A{last_row}"] = "Summary"

        if "PNL_Before_Fees" in df.columns:
            col_letter = chr(ord("A") + df.columns.get_loc("PNL_Before_Fees"))
            worksheet[f"{col_letter}{last_row}"] = df["PNL_Before_Fees"].sum()

        if "Fees" in df.columns:
            col_letter = chr(ord("A") + df.columns.get_loc("Fees"))
            worksheet[f"{col_letter}{last_row}"] = df["Fees"].sum()

        if "PNL_Net" in df.columns:
            col_letter = chr(ord("A") + df.columns.get_loc("PNL_Net"))
            worksheet[f"{col_letter}{last_row}"] = df["PNL_Net"].sum()

    return filename
