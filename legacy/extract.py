import pandas as pd

from legacy import config


def extract_orders():
    # read everything as string, cleanup happens in transform
    return pd.read_csv(config.ORDERS_FILE, dtype=str, keep_default_na=False)


def extract_customers():
    return pd.read_csv(config.CUSTOMERS_FILE, dtype=str, keep_default_na=False)


def extract_products():
    return pd.read_csv(config.PRODUCTS_FILE, dtype=str, keep_default_na=False)
