import os
import pandas as pd

INPUT = os.environ.get("INPUT_FILE", "/app/data/input.csv")
OUTPUT = os.environ.get("OUTPUT_FILE", "/app/data/output.csv")

def main():
    df = pd.read_csv(INPUT)

    # simple transformation
    df["value_x2"] = df["value"] * 2

    # write output
    df.to_csv(OUTPUT, index=False)

    print(f"Read:  {INPUT} rows={len(df)} cols={list(df.columns)}")
    print(f"Wrote: {OUTPUT}")

if __name__ == "__main__":
    main()