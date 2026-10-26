import numpy as np
import pandas as pd
 
rng = np.random.default_rng(42)
n = 2_000_000
cities = ["Melbourne", "Sydney", "Brisbane", "Perth", "Adelaide"]
cats = ["Electronics", "Clothing", "Home", "Beauty", "Sport", "Books"]
 
customers = pd.DataFrame({
    "customer_id": np.arange(1, 10_001),
    "name": [f"Customer {i}" for i in range(1, 10_001)],
    "city": rng.choice(cities, 10_000),
    "signup_date": pd.to_datetime("2022-01-01")
                   + pd.to_timedelta(rng.integers(0, 1000, 10_000), unit="D"),
})
orders = pd.DataFrame({
    "order_id": np.arange(1, n + 1),
    "customer_id": rng.integers(1, 10_001, n),
    "order_date": pd.to_datetime("2024-01-01")
                  + pd.to_timedelta(rng.integers(0, 730, n), unit="D"),
    "amount": rng.gamma(2.0, 60.0, n).round(2),
    "status": rng.choice(["done", "pending", "cancelled"], n, p=[0.8, 0.1, 0.1]),
    "category": rng.choice(cats, n),
})
orders.loc[rng.random(n) < 0.02, "amount"] = np.nan  # 2% missing amounts
 
customers.to_parquet("data/customers.parquet")
orders.to_parquet("data/orders_big.parquet")
