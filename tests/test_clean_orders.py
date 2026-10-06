import pandas as pd
from week1.clean_orders import clean_orders


def test_duplicates_are_removed() -> None:
    # Create two rows with the same order ID
    df = pd.DataFrame({
        "order_id": [1, 1],
        "customer_name": ["Alice", "Alice"],
        "order_date": ["01/02/2026", "01/02/2026"],
        "amount": ["$10.00", "$10.00"],
        "status": ["paid", "paid"],
        "city": ["Melbourne", "Melbourne"]
    })

    # Run your cleaning function
    result = clean_orders(df)

    # Check that only one row remains
    assert len(result) == 1

def test_dollar_signs_are_removed() -> None:
    df = pd.DataFrame({
        "order_id": [1, 2],
        "customer_name": ["Alice", "Bob"],
        "order_date": ["01/02/2026", "02/02/2026"],
        "amount": ["$10.50", "$20.00"],
        "status": ["paid", "paid"],
        "city": ["Melbourne", "Sydney"]
    })

    result = clean_orders(df)

    assert len(result) == 2