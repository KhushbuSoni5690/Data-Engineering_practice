import csv, random
from datetime import date, timedelta
 
random.seed(1)
names = ["Aisha Khan", "ravi patel ", " Mei Chen", "Tom Nguyen", "Sara Lopez"]
cities = ["Melbourne", "Sydney", "Brisbane", "Perth", ""]
rows = []
for i in range(1, 501):
    d = date(2025, 1, 1) + timedelta(days=random.randint(0, 364))
    fmt = random.choice(["%Y-%m-%d", "%d/%m/%Y", "%d %b %Y"])
    amt = random.choice([f"{random.uniform(5, 500):.2f}", f"${random.uniform(5, 500):.2f}", ""])
    rows.append([i, random.choice(names), d.strftime(fmt), amt,
                 random.choice(cities), random.choice(["done", "DONE", "pending", "cancelled"])])
rows += rows[:20]  # duplicate rows on purpose
random.shuffle(rows)
with open("orders_raw.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["order_id", "customer_name", "order_date", "amount", "city", "status"])
    w.writerows(rows)

