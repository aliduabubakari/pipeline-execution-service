import os
import pandas as pd


INPUT_FILE = os.environ.get("INPUT_FILE", "/app/data/orders_enriched.csv")
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "/app/data/daily_sales_summary.csv")


def main() -> None:
    df = pd.read_csv(INPUT_FILE)

    summary = (
        df.groupby(["order_date", "category"], dropna=False)
        .agg(
            orders=("order_id", "count"),
            units_sold=("quantity", "sum"),
            revenue=("line_total", "sum"),
        )
        .reset_index()
        .sort_values(["order_date", "revenue"], ascending=[True, False])
    )
    summary["revenue"] = summary["revenue"].round(2)

    daily_totals = (
        df.groupby(["order_date"], dropna=False)
        .agg(
            orders=("order_id", "count"),
            units_sold=("quantity", "sum"),
            revenue=("line_total", "sum"),
        )
        .reset_index()
    )
    daily_totals["category"] = "ALL"
    daily_totals["revenue"] = daily_totals["revenue"].round(2)

    output = pd.concat([summary, daily_totals], ignore_index=True)
    output = output.sort_values(["order_date", "category"]).reset_index(drop=True)
    output.to_csv(OUTPUT_FILE, index=False)

    print(f"Summary rows: {len(output)}")
    print(f"Total revenue: {output.loc[output['category'] == 'ALL', 'revenue'].sum():.2f}")
    print(f"Wrote {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
