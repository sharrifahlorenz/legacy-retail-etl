"""Business transformations for the nightly retail batch.

Written in 2019 for a single on-prem box. Runs in-memory with pandas and
row-by-row loops; takes ~40 min on full production volume and OOMs on
Black Friday week.
"""
from datetime import datetime

import pandas as pd

from legacy import config


def _parse_date(s):
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def clean_orders(orders):
    df = orders.copy()
    df = df.drop_duplicates()

    # row-by-row date parsing (slow!)
    parsed = []
    for _, row in df.iterrows():
        parsed.append(_parse_date(row["order_date"]))
    df["order_date"] = parsed
    df = df[df["order_date"].notnull()]

    df["order_id"] = df["order_id"].astype(int)
    df["line_no"] = df["line_no"].astype(int)
    df["customer_id"] = df["customer_id"].astype(int)
    df["product_id"] = df["product_id"].astype(int)
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)
    df["discount_pct"] = df["discount_pct"].replace("", "0").astype(float)
    df["channel"] = df["channel"].str.strip().str.lower()

    df["gross_amount"] = (df["quantity"] * df["unit_price"]).round(2)
    df["net_amount"] = (df["gross_amount"] * (1 - df["discount_pct"])).round(2)
    df["is_return"] = df["quantity"] < 0
    return df.reset_index(drop=True)


def clean_customers(customers):
    df = customers.copy()
    df["customer_id"] = df["customer_id"].astype(int)
    df["region"] = df["region"].str.strip().str.upper()
    df.loc[df["region"] == "", "region"] = "UNKNOWN"
    df["email"] = df["email"].replace("", None)
    df["signup_date"] = pd.to_datetime(df["signup_date"]).dt.date
    return df


def clean_products(products):
    df = products.copy()
    df["product_id"] = df["product_id"].astype(int)
    df["unit_cost"] = df["unit_cost"].astype(float)
    df["list_price"] = df["list_price"].astype(float)
    return df


def build_order_facts(orders, customers, products):
    # inner join drops orphan customer ids (known data issue, ticket DATA-212)
    df = orders.merge(customers[["customer_id", "region"]], on="customer_id", how="inner")
    df = df.merge(products[["product_id", "category", "unit_cost"]], on="product_id", how="left")
    df["cost_amount"] = (df["quantity"] * df["unit_cost"]).round(2)
    df["margin_amount"] = (df["net_amount"] - df["cost_amount"]).round(2)
    return df


def daily_sales_by_region_category(facts):
    agg = (
        facts.groupby(["order_date", "region", "category"])
        .agg(
            orders=("order_id", "nunique"),
            units=("quantity", "sum"),
            net_revenue=("net_amount", "sum"),
            margin=("margin_amount", "sum"),
        )
        .reset_index()
    )
    agg["net_revenue"] = agg["net_revenue"].round(2)
    agg["margin"] = agg["margin"].round(2)
    return agg.sort_values(["order_date", "region", "category"]).reset_index(drop=True)


def customer_lifetime_value(facts):
    rows = []
    # loop over customers one at a time -- does not scale
    for cid, grp in facts.groupby("customer_id"):
        net = round(grp["net_amount"].sum(), 2)
        if net >= config.HIGH_VALUE_THRESHOLD:
            tier = "VIP"
        elif net >= config.MID_VALUE_THRESHOLD:
            tier = "MID"
        else:
            tier = "STANDARD"
        rows.append({
            "customer_id": cid,
            "region": grp["region"].iloc[0],
            "first_order_date": grp["order_date"].min(),
            "last_order_date": grp["order_date"].max(),
            "order_count": grp["order_id"].nunique(),
            "lifetime_net_revenue": net,
            "return_lines": int(grp["is_return"].sum()),
            "tier": tier,
        })
    return pd.DataFrame(rows).sort_values("customer_id").reset_index(drop=True)
