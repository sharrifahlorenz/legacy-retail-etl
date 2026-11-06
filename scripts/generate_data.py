"""Generate deterministic sample raw data for the nightly retail ETL.

Produces intentionally messy CSV extracts (duplicates, nulls, bad dates,
returns with negative quantities, inconsistent casing) that mirror what the
upstream POS / e-commerce systems drop into the landing folder each night.

Usage: python scripts/generate_data.py
"""
import csv
import os
import random
from datetime import date, timedelta

SEED = 42
N_CUSTOMERS = 800
N_PRODUCTS = 120
N_ORDERS = 12000

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(ROOT, "data", "raw")

REGIONS = ["NORTHEAST", "Southeast", "midwest", "West", "SOUTHWEST", " West "]
CATEGORIES = ["Electronics", "Home", "Apparel", "Grocery", "Toys", "Sports"]
CHANNELS = ["web", "store", "mobile", "WEB", "Store"]


def main():
    rng = random.Random(SEED)
    os.makedirs(RAW, exist_ok=True)

    # customers
    with open(os.path.join(RAW, "customers.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "first_name", "last_name", "email", "region", "signup_date"])
        for cid in range(1, N_CUSTOMERS + 1):
            signup = date(2021, 1, 1) + timedelta(days=rng.randint(0, 1000))
            email = "cust%d@example.com" % cid if rng.random() > 0.03 else ""
            region = rng.choice(REGIONS) if rng.random() > 0.02 else ""
            w.writerow([cid, "First%d" % cid, "Last%d" % cid, email, region, signup.isoformat()])

    # products
    prices = {}
    with open(os.path.join(RAW, "products.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "sku", "product_name", "category", "unit_cost", "list_price"])
        for pid in range(1, N_PRODUCTS + 1):
            cost = round(rng.uniform(2, 400), 2)
            price = round(cost * rng.uniform(1.15, 2.2), 2)
            prices[pid] = price
            w.writerow([pid, "SKU-%05d" % pid, "Product %d" % pid, rng.choice(CATEGORIES), cost, price])

    # orders (one row per order line)
    rows = []
    start = date(2024, 1, 1)
    for oid in range(1, N_ORDERS + 1):
        d = start + timedelta(days=rng.randint(0, 364))
        cid = rng.randint(1, N_CUSTOMERS + 5)  # a few orphan customer ids
        n_lines = rng.randint(1, 4)
        channel = rng.choice(CHANNELS)
        for line in range(1, n_lines + 1):
            pid = rng.randint(1, N_PRODUCTS)
            qty = rng.randint(1, 5)
            if rng.random() < 0.04:
                qty = -qty  # return
            price = prices[pid]
            if rng.random() < 0.1:
                price = round(price * rng.choice([0.8, 0.9]), 2)  # promo
            discount = "" if rng.random() > 0.15 else round(rng.uniform(0, 0.25), 2)
            if rng.random() < 0.01:
                order_date = "not-a-date"
            elif rng.random() < 0.3:
                order_date = d.strftime("%m/%d/%Y")  # legacy POS format
            else:
                order_date = d.isoformat()
            rows.append([oid, line, cid, pid, qty, price, discount, order_date, channel])

    # inject exact duplicate lines (double-delivered files)
    dups = rng.sample(rows, 150)
    rows.extend(dups)
    rng.shuffle(rows)

    with open(os.path.join(RAW, "orders.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "line_no", "customer_id", "product_id", "quantity",
                    "unit_price", "discount_pct", "order_date", "channel"])
        w.writerows(rows)

    print("wrote raw data to", RAW)


if __name__ == "__main__":
    main()
