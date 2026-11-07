"""Silver: typed, cleaned and conformed entities.

Each function ports the matching function in ``legacy/transform.py`` using native Spark
expressions only (no Python UDFs, no ``toPandas()``, no row loops).
"""
import logging

from pyspark.sql import Window
from pyspark.sql import functions as F

from databricks_pipeline.bronze import bronze_table
from databricks_pipeline.config import SOURCES
from databricks_pipeline.delta_io import ensure_schema, overwrite_table

log = logging.getLogger(__name__)

ISO_DATE = r"^(\d{4})-(\d{1,2})-(\d{1,2})$"  # strptime %Y-%m-%d
US_DATE = r"^(\d{1,2})/(\d{1,2})/(\d{4})$"  # strptime %m/%d/%Y


def silver_table(cfg, entity):
    return cfg.table(f"silver_{entity}")


def strip(col):
    """Python ``str.strip()``: trims all leading/trailing whitespace, not just spaces."""
    return F.regexp_replace(col, r"^\s+|\s+$", "")


def blank_to_null(col):
    return F.when(strip(col) != "", col)


def round_half_even_2dp(col):
    """Bit-for-bit equivalent of pandas/numpy ``round(2)`` on float64: ``rint(x * 100) / 100``."""
    return F.rint(col * 100.0) / 100.0


def _make_date_checked(year, month, day):
    # Validate before make_date so invalid calendar dates (e.g. 2024-02-30) become NULL
    # instead of raising under ANSI mode, matching strptime's ValueError -> None.
    y, m, d = year.cast("int"), month.cast("int"), day.cast("int")
    last_dom = F.dayofmonth(F.last_day(F.make_date(y, m, F.lit(1))))
    return F.when((y >= 1) & m.between(1, 12), F.when(d.between(1, last_dom), F.make_date(y, m, d)))


def parse_date(col):
    """Port of ``legacy.transform._parse_date``: ``YYYY-MM-DD`` or ``MM/DD/YYYY``, else NULL.

    Implemented with regex + make_date instead of to_date so the result does not depend on
    ``spark.sql.legacy.timeParserPolicy`` or ANSI mode.
    """
    s = strip(F.coalesce(col, F.lit("")))
    return F.when(
        s.rlike(ISO_DATE),
        _make_date_checked(*(F.regexp_extract(s, ISO_DATE, i) for i in (1, 2, 3))),
    ).when(
        s.rlike(US_DATE),
        _make_date_checked(*(F.regexp_extract(s, US_DATE, i) for i in (3, 1, 2))),
    )


def clean_orders(orders):
    """Port of ``legacy.transform.clean_orders``."""
    raw_cols = SOURCES["orders"]
    df = (
        orders.select(*raw_cols)
        .dropDuplicates(raw_cols)  # exact duplicate lines (double-delivered files)
        .withColumn("order_date", parse_date(F.col("order_date")))
        .where(F.col("order_date").isNotNull())  # unparseable dates are dropped
    )
    discount = blank_to_null(F.col("discount_pct"))
    df = df.select(
        F.col("order_id").cast("bigint").alias("order_id"),
        F.col("line_no").cast("int").alias("line_no"),
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.col("product_id").cast("bigint").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("double").alias("unit_price"),
        F.coalesce(discount.cast("double"), F.lit(0.0)).alias("discount_pct"),  # blank = 0
        F.col("order_date"),
        F.lower(strip(F.col("channel"))).alias("channel"),
    )
    gross = round_half_even_2dp(F.col("quantity") * F.col("unit_price"))
    return (
        df.withColumn("gross_amount", gross)
        .withColumn("net_amount", round_half_even_2dp(F.col("gross_amount") * (1 - F.col("discount_pct"))))
        .withColumn("is_return", F.col("quantity") < 0)  # returns stay in revenue and net it down
    )


def latest_per_key(df, key):
    """Keep the most recently ingested row per key.

    Bronze is append-only, so master-data snapshots accumulate across nights. With a single
    snapshot (as in the legacy job) this is a no-op.
    """
    if "_ingested_at" not in df.columns:
        return df
    w = Window.partitionBy(key).orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def clean_customers(customers):
    """Port of ``legacy.transform.clean_customers``."""
    df = latest_per_key(customers, "customer_id")
    region = F.upper(strip(F.col("region")))
    return df.select(
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        blank_to_null(F.col("email")).alias("email"),
        F.when(F.coalesce(region, F.lit("")) == "", F.lit("UNKNOWN")).otherwise(region).alias("region"),
        parse_date(F.col("signup_date")).alias("signup_date"),
    )


def clean_products(products):
    """Port of ``legacy.transform.clean_products``."""
    df = latest_per_key(products, "product_id")
    return df.select(
        F.col("product_id").cast("bigint").alias("product_id"),
        F.col("sku"),
        F.col("product_name"),
        F.col("category"),
        F.col("unit_cost").cast("double").alias("unit_cost"),
        F.col("list_price").cast("double").alias("list_price"),
    )


CLEANERS = {"orders": clean_orders, "customers": clean_customers, "products": clean_products}


def run(spark, cfg):
    ensure_schema(spark, cfg)
    for entity, clean in CLEANERS.items():
        log.info("silver %s", entity)
        overwrite_table(clean(spark.table(bronze_table(cfg, entity))), silver_table(cfg, entity))
