"""Entry point invoked by cron on the on-prem batch server (see crontab.txt).

    python -m legacy.run_nightly
"""
import logging
import time

from legacy import extract, load, transform

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nightly")


def main():
    t0 = time.time()
    orders = transform.clean_orders(extract.extract_orders())
    customers = transform.clean_customers(extract.extract_customers())
    products = transform.clean_products(extract.extract_products())
    log.info("cleaned %d order lines", len(orders))

    facts = transform.build_order_facts(orders, customers, products)
    daily = transform.daily_sales_by_region_category(facts)
    clv = transform.customer_lifetime_value(facts)

    load.load_table(facts, "fct_order_lines")
    load.load_table(daily, "agg_daily_sales")
    load.load_table(clv, "dim_customer_value")
    log.info("nightly batch finished in %.1fs", time.time() - t0)


if __name__ == "__main__":
    main()
