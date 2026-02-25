Practical declarative demo package for PES (manifest-only).

Pipeline:
1. `clean_orders` - cleans and validates raw order rows
2. `enrich_orders` - joins product metadata and computes `line_total`
3. `summarize_daily_sales` - writes category + daily totals summary

Expected outputs in `/app/data` after a successful run:
- `orders_clean.csv`
- `orders_enriched.csv`
- `daily_sales_summary.csv`
