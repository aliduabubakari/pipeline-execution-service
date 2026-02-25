import os
import pandas as pd


INPUT_FILE = os.environ.get("INPUT_FILE", "/app/data/orders_raw.csv")
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "/app/data/orders_clean.csv")


def main() -> None:
    df = pd.read_csv(INPUT_FILE)

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date.astype(str)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["customer_region"] = df["customer_region"].astype(str).str.strip().str.title()

    before = len(df)
    df = df.dropna(subset=["order_date", "product_id", "unit_price"])
    df = df[(df["quantity"] > 0) & (df["unit_price"] > 0)]
    df = df.drop_duplicates(subset=["order_id"])
    after = len(df)

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Cleaned orders: {before} -> {after}")
    print(f"Wrote {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
