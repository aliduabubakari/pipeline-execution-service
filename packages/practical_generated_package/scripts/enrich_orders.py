import os
import pandas as pd


ORDERS_FILE = os.environ.get("ORDERS_FILE", "/app/data/orders_clean.csv")
PRODUCTS_FILE = os.environ.get("PRODUCTS_FILE", "/app/data/products.csv")
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "/app/data/orders_enriched.csv")


def main() -> None:
    orders = pd.read_csv(ORDERS_FILE)
    products = pd.read_csv(PRODUCTS_FILE)

    enriched = orders.merge(products, on="product_id", how="left")
    enriched["category"] = enriched["category"].fillna("Unknown")
    enriched["product_name"] = enriched["product_name"].fillna("Unknown Product")
    enriched["line_total"] = (enriched["quantity"] * enriched["unit_price"]).round(2)

    enriched.to_csv(OUTPUT_FILE, index=False)
    missing = int((enriched["category"] == "Unknown").sum())
    print(f"Enriched rows: {len(enriched)} (unknown products: {missing})")
    print(f"Wrote {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
