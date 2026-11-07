"""Unit tests for the silver-layer ports of the legacy business rules."""
import random

import numpy as np
import pytest
from pyspark.sql import functions as F

from databricks_pipeline import gold, silver
from databricks_pipeline.config import SOURCES, PipelineConfig
from legacy import transform

ORDER_COLS = SOURCES["orders"]


def schema_of(cols):
    return ", ".join(f"{c} string" for c in cols)


def orders_df(spark, rows):
    return spark.createDataFrame(rows, schema_of(ORDER_COLS))


def order(order_id="1", line_no="1", customer_id="1", product_id="1", quantity="2",
          unit_price="10.00", discount_pct=None, order_date="2024-01-01", channel="web"):
    return (order_id, line_no, customer_id, product_id, quantity, unit_price, discount_pct,
            order_date, channel)


@pytest.mark.parametrize("raw", [
    "2024-03-05", "03/05/2024", "2024-3-5", "3/5/2024", "  2024-03-05\t", "2024-02-29", "12/31/2024",
    "2023-02-29", "2024-13-01", "2024-04-31", "13/01/2024", "0000-01-01", "2024-03-05x", "24-03-05",
    "2024/03/05", "not-a-date", "", "   ",
])
def test_parse_date_matches_legacy(spark, raw):
    got = spark.createDataFrame([(raw,)], ["d"]).select(silver.parse_date(F.col("d"))).first()[0]
    assert got == transform._parse_date(raw)


def test_parse_date_null_is_null(spark):
    df = spark.createDataFrame([(None,)], "d string")
    assert df.select(silver.parse_date(F.col("d"))).first()[0] is None


def test_exact_duplicates_dropped_but_near_duplicates_kept(spark):
    rows = [order(), order(), order(order_date="01/01/2024")]
    out = silver.clean_orders(orders_df(spark, rows))
    assert out.count() == 2


def test_bad_dates_dropped(spark):
    rows = [order(order_id="1"), order(order_id="2", order_date="not-a-date"),
            order(order_id="3", order_date=None), order(order_id="4", order_date="02/30/2024")]
    out = silver.clean_orders(orders_df(spark, rows))
    assert [r.order_id for r in out.collect()] == [1]


def test_returns_flagged_and_net_down_revenue(spark):
    rows = [order(order_id="1", quantity="-3", unit_price="9.99", discount_pct="0.1")]
    r = silver.clean_orders(orders_df(spark, rows)).first()
    assert r.is_return is True
    assert r.gross_amount == -29.97
    assert r.net_amount == -26.97


def test_blank_discount_is_zero(spark):
    rows = [order(order_id="1", discount_pct=None), order(order_id="2", discount_pct=""),
            order(order_id="3", discount_pct="  "), order(order_id="4", discount_pct="0.25")]
    out = {r.order_id: (r.discount_pct, r.net_amount)
           for r in silver.clean_orders(orders_df(spark, rows)).collect()}
    assert out == {1: (0.0, 20.0), 2: (0.0, 20.0), 3: (0.0, 20.0), 4: (0.25, 15.0)}


def test_channel_normalised(spark):
    rows = [order(order_id=str(i), channel=c) for i, c in enumerate(["WEB", " Store ", "mobile\t"])]
    out = sorted(r.channel for r in silver.clean_orders(orders_df(spark, rows)).collect())
    assert out == ["mobile", "store", "web"]


def test_types(spark):
    out = silver.clean_orders(orders_df(spark, [order()]))
    assert dict(out.dtypes) == {
        "order_id": "bigint", "line_no": "int", "customer_id": "bigint", "product_id": "bigint",
        "quantity": "int", "unit_price": "double", "discount_pct": "double", "order_date": "date",
        "channel": "string", "gross_amount": "double", "net_amount": "double", "is_return": "boolean",
    }


def test_money_rounding_is_bit_identical_to_pandas(spark):
    rng = random.Random(7)
    rows = []
    for i in range(3000):
        qty = rng.choice([-5, -1, 1, 2, 3, 4, 5, 7])
        price = round(rng.uniform(0.01, 999), rng.choice([2, 3]))
        disc = rng.choice(["", "0.05", str(round(rng.uniform(0, 0.25), 2)), "0.125"])
        rows.append(order(order_id=str(i), quantity=str(qty), unit_price=repr(price), discount_pct=disc))
    import pandas as pd

    expected = transform.clean_orders(pd.DataFrame(rows, columns=ORDER_COLS))
    got = silver.clean_orders(orders_df(spark, [tuple("" if v is None else v for v in r) for r in rows]))
    got = got.select("order_id", "gross_amount", "net_amount").toPandas().sort_values("order_id")
    expected = expected.sort_values("order_id")
    assert np.array_equal(got["gross_amount"].to_numpy(), expected["gross_amount"].to_numpy())
    assert np.array_equal(got["net_amount"].to_numpy(), expected["net_amount"].to_numpy())


def customers_df(spark, rows):
    return spark.createDataFrame(rows, schema_of(SOURCES["customers"]))


def test_region_normalised_and_blank_is_unknown(spark):
    rows = [("1", "a", "b", "x@y", " West ", "2021-01-01"), ("2", "a", "b", "", "midwest", "2021-01-01"),
            ("3", "a", "b", None, "", "2021-01-01"), ("4", "a", "b", None, None, "2021-01-01"),
            ("5", "a", "b", None, "   ", "2021-01-01")]
    out = {r.customer_id: (r.region, r.email) for r in silver.clean_customers(customers_df(spark, rows)).collect()}
    assert out == {1: ("WEST", "x@y"), 2: ("MIDWEST", None), 3: ("UNKNOWN", None),
                   4: ("UNKNOWN", None), 5: ("UNKNOWN", None)}


def test_latest_customer_snapshot_wins(spark):
    df = customers_df(spark, [("1", "a", "b", None, "West", "2021-01-01"),
                              ("1", "a", "b", None, "East", "2021-01-01")])
    df = df.withColumn("_source_file", F.lit("f")).withColumn(
        "_ingested_at", F.when(F.col("region") == "East", F.lit("2024-01-02")).otherwise(F.lit("2024-01-01"))
        .cast("timestamp"))
    assert [r.region for r in silver.clean_customers(df).collect()] == ["EAST"]


def test_inner_join_drops_orphan_customers_and_left_join_products(spark):
    orders = silver.clean_orders(orders_df(spark, [order(order_id="1", customer_id="1", product_id="1"),
                                                   order(order_id="2", customer_id="999"),
                                                   order(order_id="3", customer_id="1", product_id="42")]))
    customers = silver.clean_customers(customers_df(spark, [("1", "a", "b", None, "west", "2021-01-01")]))
    products = silver.clean_products(spark.createDataFrame(
        [("1", "SKU-1", "P1", "Toys", "4.005", "10")], schema_of(SOURCES["products"])))
    facts = {r.order_id: r for r in gold.build_order_facts(orders, customers, products).collect()}
    assert set(facts) == {1, 3}
    assert facts[1].region == "WEST" and facts[1].category == "Toys"
    assert str(facts[1].cost_amount) == "8.01" and str(facts[1].margin_amount) == "11.99"
    assert facts[3].category is None and facts[3].margin_amount is None

    daily = gold.daily_sales_by_region_category(gold.build_order_facts(orders, customers, products))
    assert daily.count() == 1  # NULL category grouped out, like pandas groupby


def test_tier_thresholds_come_from_config(spark):
    orders = silver.clean_orders(orders_df(spark, [order(customer_id="1", quantity="1", unit_price="100"),
                                                   order(customer_id="2", quantity="1", unit_price="50"),
                                                   order(customer_id="3", quantity="1", unit_price="10")]))
    customers = silver.clean_customers(customers_df(spark, [(str(i), "a", "b", None, "W", "2021-01-01")
                                                            for i in (1, 2, 3)]))
    products = silver.clean_products(spark.createDataFrame([("1", "S", "P", "Toys", "1", "2")],
                                                           schema_of(SOURCES["products"])))
    facts = gold.build_order_facts(orders, customers, products)
    cfg = PipelineConfig(vip_threshold=100, mid_threshold=50)
    tiers = {r.customer_id: r.tier for r in
             gold.customer_lifetime_value(facts, cfg.vip_threshold, cfg.mid_threshold).collect()}
    assert tiers == {1: "VIP", 2: "MID", 3: "STANDARD"}


def test_config_rejects_inverted_thresholds():
    with pytest.raises(ValueError):
        PipelineConfig(vip_threshold=10, mid_threshold=20)


def test_config_from_env(monkeypatch):
    monkeypatch.setenv("RETAIL_VIP_THRESHOLD", "50000")
    monkeypatch.setenv("RETAIL_CATALOG", "dev")
    cfg = PipelineConfig.from_env(schema="s")
    assert (cfg.catalog, cfg.schema, cfg.vip_threshold, cfg.mid_threshold) == ("dev", "s", 50000.0, 25000.0)
