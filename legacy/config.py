# Legacy config -- paths are hard-coded for the on-prem batch server.
# TODO(2019): move to env vars
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "output")
WAREHOUSE_DB = os.path.join(OUTPUT_DIR, "warehouse.db")

ORDERS_FILE = os.path.join(RAW_DIR, "orders.csv")
CUSTOMERS_FILE = os.path.join(RAW_DIR, "customers.csv")
PRODUCTS_FILE = os.path.join(RAW_DIR, "products.csv")

# business rules
HIGH_VALUE_THRESHOLD = 40000.0  # lifetime net revenue for "VIP" tier
MID_VALUE_THRESHOLD = 25000.0
