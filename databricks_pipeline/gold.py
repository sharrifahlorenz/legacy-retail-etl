"""Gold: the three legacy output tables, written as Delta tables to ``<catalog>.<schema>``."""
import logging

from pyspark.sql import functions as F

from databricks_pipeline.delta_io import ensure_schema, merge_table, overwrite_table
from databricks_pipeline.silver import round_half_even_2dp, silver_table

log = logging.getLogger(__name__)

# Money amounts are stored as DECIMAL so aggregates are exact (legacy used float64).
MONEY = "decimal(18,2)"


def build_order_facts(orders, customers, products):
    """Port of ``legacy.transform.build_order_facts``."""
    # inner join drops orphan customer ids (known data issue, ticket DATA-212)
    df = orders.join(customers.select("customer_id", "region"), "customer_id", "inner")
    df = df.join(products.select("product_id", "category", "unit_cost"), "product_id", "left")
    cost = round_half_even_2dp(F.col("quantity") * F.col("unit_cost"))
    margin = round_half_even_2dp(F.col("net_amount") - cost)
    return df.select(
        "order_id", "line_no", "customer_id", "product_id", "quantity", "unit_price",
        "discount_pct", "order_date", "channel",
        F.col("gross_amount").cast(MONEY).alias("gross_amount"),
        F.col("net_amount").cast(MONEY).alias("net_amount"),
        "is_return", "region", "category", "unit_cost",
        cost.cast(MONEY).alias("cost_amount"),
        margin.cast(MONEY).alias("margin_amount"),
    )


def daily_sales_by_region_category(facts):
    """Port of ``legacy.transform.daily_sales_by_region_category``."""
    keys = ["order_date", "region", "category"]
    return (
        # pandas groupby drops NULL keys (e.g. order lines for an unknown product)
        facts.where(" AND ".join(f"{k} IS NOT NULL" for k in keys))
        .groupBy(*keys)
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units"),
            F.sum("net_amount").cast(MONEY).alias("net_revenue"),
            F.sum("margin_amount").cast(MONEY).alias("margin"),
        )
    )


def customer_lifetime_value(facts, vip_threshold, mid_threshold):
    """Port of ``legacy.transform.customer_lifetime_value`` with thresholds from config."""
    net = F.col("lifetime_net_revenue")
    return (
        facts.groupBy("customer_id")
        .agg(
            F.min("region").alias("region"),  # region is a function of customer_id
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("order_id").alias("order_count"),
            F.sum("net_amount").cast(MONEY).alias("lifetime_net_revenue"),
            F.sum(F.col("is_return").cast("int")).alias("return_lines"),
        )
        .withColumn(
            "tier",
            F.when(net >= F.lit(vip_threshold), "VIP")
            .when(net >= F.lit(mid_threshold), "MID")
            .otherwise("STANDARD"),
        )
    )


def build(spark, cfg):
    facts = build_order_facts(
        spark.table(silver_table(cfg, "orders")),
        spark.table(silver_table(cfg, "customers")),
        spark.table(silver_table(cfg, "products")),
    )
    return {
        "fct_order_lines": facts,
        "agg_daily_sales": daily_sales_by_region_category(facts),
        "dim_customer_value": customer_lifetime_value(facts, cfg.vip_threshold, cfg.mid_threshold),
    }


def run(spark, cfg):
    ensure_schema(spark, cfg)
    tables = build(spark, cfg)
    # Full recompute from silver each night. Order lines have no guaranteed unique key (legacy keeps
    # non-identical re-sends of a line), so the fact and the aggregate are atomically replaced;
    # the customer dimension is merged on its key so unchanged customers are not rewritten.
    overwrite_table(tables["fct_order_lines"], cfg.table("fct_order_lines"))
    overwrite_table(tables["agg_daily_sales"], cfg.table("agg_daily_sales"))
    merge_table(spark, tables["dim_customer_value"], cfg.table("dim_customer_value"), ["customer_id"])
