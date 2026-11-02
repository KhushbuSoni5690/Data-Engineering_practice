"""Clean CSV orders without inventing missing revenue."""
import argparse
import logging
from pathlib import Path

import pandas as pd


def load_data(input_path: str) -> pd.DataFrame:
    logging.info("Loading orders from %s", input_path)
    return pd.read_csv(input_path)


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    duplicate_count = df.duplicated(subset=["order_id"]).sum()
    if duplicate_count:
        logging.warning("Found %s duplicate order IDs", duplicate_count)
    cleaned = df.drop_duplicates(subset=["order_id"]).copy()
    cleaned["customer_name"] = cleaned["customer_name"].str.strip().str.title()
    cleaned["order_date"] = pd.to_datetime(
        cleaned["order_date"], format="mixed", dayfirst=True
    )
    amount_text = cleaned["amount"].astype("string").str.replace("$", "", regex=False)
    cleaned["amount"] = pd.to_numeric(amount_text, errors="raise").astype(float).round(2)
    cleaned["status"] = cleaned["status"].str.strip().str.lower()
    cleaned["city"] = cleaned["city"].fillna("unknown")
    cleaned["is_amount_missing"] = cleaned["amount"].isna()
    missing_count = int(cleaned["is_amount_missing"].sum())
    if missing_count:
        logging.warning("Found %s missing amounts; keeping them as null", missing_count)
    return cleaned


def save_data(df: pd.DataFrame, output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logging.info("Cleaned orders saved to %s", output_path)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    save_data(clean_orders(load_data(args.input)), args.output)


if __name__ == "__main__":
    main()
