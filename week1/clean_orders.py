
import pandas as pd
import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)

def load_data(input_path: str) -> pd.DataFrame:
    df = pd.read_csv(input_path)
    return df


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    duplicate_count = df.duplicated(subset=["order_id"]).sum()
    if duplicate_count > 0:
        logging.warning("Found %s duplicate order IDs", duplicate_count)

    cleaned_df = df.drop_duplicates(subset=["order_id"]).copy()

    # naming convention
    cleaned_df['customer_name'] = cleaned_df['customer_name'].str.strip().str.title()
    # cleaned_df['customer_names'] = cleaned_df['customer_names'].str.strip().str.title()

    #date format globalisation
    cleaned_df['order_date'] = pd.to_datetime(cleaned_df['order_date'], format='mixed', dayfirst=True)

    #amount $ sign remove
    # Complete workflow hint:
    # df['amount'] = df['amount'].str.replace('$', '', regex=False).astype(float).round(2)
    cleaned_df['amount'] = cleaned_df['amount'].str.replace('$', '', regex = False).astype(float).round(2)

    # status lower case
    cleaned_df['status'] = cleaned_df['status'].str.strip().str.lower()

    # Run this print statement at the end of your script to verify:
    logging.info(cleaned_df['status'].unique())

    # missing cities replaced with unknown for clarity
    cleaned_df['city'] = cleaned_df['city'].fillna('unknown')

    missing_amount_count = cleaned_df["amount"].isna().sum()
    if missing_amount_count > 0:
        logging.warning("Found %s missing amounts", missing_amount_count)
    #missing amount will be replaced by avg og the amount.
    avg_amount = cleaned_df['amount'].mean()
    avg_amount_rounded = round(avg_amount, 2) 
    cleaned_df['amount'] = cleaned_df['amount'].fillna(avg_amount_rounded)


    return cleaned_df

def save_data(df: pd.DataFrame, output_path: str) -> None:
    df.to_parquet(output_path, index=False) 


def main() -> None:
    
    parser = argparse.ArgumentParser(description="Clean order data")

    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    df = load_data(args.input)
    cleaned_df = clean_orders(df)
    save_data(cleaned_df, args.output)

    logging.info("Success! Cleaned data saved.")


if __name__ == "__main__":
    main()




