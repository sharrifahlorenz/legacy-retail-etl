"""Parity: the Lakehouse pipeline on local Spark must reproduce the legacy golden outputs."""
import os

import pandas as pd
import pytest

from databricks_pipeline import main as pipeline
from databricks_pipeline.config import SOURCES, PipelineConfig
from legacy import extract, transform
from tests.conftest import EXPECTED_DIR, RAW_DIR

MONEY_TOL = 0.01
KEYS = {
    "agg_daily_sales": ["order_date", "region", "category"],
    "dim_customer_value": ["customer_id"],
}
MONEY_COLS = {
    "agg_daily_sales": ["net_revenue", "margin"],
    "dim_customer_value": ["lifetime_net_revenue"],
}


@pytest.fixture(scope="module")
def cfg():
    return PipelineConfig(catalog="spark_catalog", schema="retail_parity", source_path=RAW_DIR,
                          ingest_mode="batch")


@pytest.fixture(scope="module")
def pipeline_run(spark, cfg):
    pipeline.run(spark, cfg)
    return cfg


def read_gold(spark, cfg, table):
    df = spark.table(cfg.table(table)).toPandas()
    for c in df.columns:
        if c.endswith("_date"):
            df[c] = df[c].astype(str)
    return df


def assert_frame_parity(actual, expected, keys, money_cols):
    assert list(actual.columns) == list(expected.columns)
    assert len(actual) == len(expected)
    actual = actual.astype({c: (float if c in money_cols else expected[c].dtype) for c in expected.columns})
    actual = actual.sort_values(keys).reset_index(drop=True)
    expected = expected.sort_values(keys).reset_index(drop=True)
    for col in expected.columns:
        if col in money_cols:
            diff = (actual[col] - expected[col].astype(float)).abs()
            assert diff.max() <= MONEY_TOL, f"{col}: max abs diff {diff.max()}"
        else:
            mismatched = actual[col] != expected[col]
            assert not mismatched.any(), f"{col} differs on {int(mismatched.sum())} rows:\n" \
                f"{actual[mismatched].head()}\nexpected:\n{expected[mismatched].head()}"


@pytest.mark.parametrize("table", ["agg_daily_sales", "dim_customer_value"])
def test_gold_matches_legacy_golden_snapshot(spark, pipeline_run, table):
    expected = pd.read_csv(os.path.join(EXPECTED_DIR, f"{table}.csv"))
    actual = read_gold(spark, pipeline_run, table)
    assert_frame_parity(actual, expected, KEYS[table], MONEY_COLS[table])


def test_fct_order_lines_matches_legacy(spark, pipeline_run):
    legacy_facts = transform.build_order_facts(
        transform.clean_orders(extract.extract_orders()),
        transform.clean_customers(extract.extract_customers()),
        transform.clean_products(extract.extract_products()),
    )
    legacy_facts["order_date"] = legacy_facts["order_date"].astype(str)
    actual = read_gold(spark, pipeline_run, "fct_order_lines")
    keys = SOURCES["orders"]
    assert_frame_parity(actual, legacy_facts, keys,
                        ["gross_amount", "net_amount", "cost_amount", "margin_amount"])


def test_bronze_is_raw_strings_with_metadata(spark, pipeline_run):
    for source, cols in SOURCES.items():
        df = spark.table(pipeline_run.table(f"bronze_{source}"))
        assert df.columns == cols + ["_ingested_at", "_source_file"]
        assert all(t == "string" for c, t in df.dtypes if c in cols)
        with open(os.path.join(RAW_DIR, f"{source}.csv")) as f:
            assert df.count() == sum(1 for _ in f) - 1
        assert df.where(~df["_source_file"].endswith(f"/{source}.csv")).count() == 0


def test_rerun_is_idempotent(spark, pipeline_run):
    tables = ["bronze_orders", "silver_orders", "fct_order_lines", "agg_daily_sales", "dim_customer_value"]
    before = {t: spark.table(pipeline_run.table(t)).drop("_ingested_at") for t in tables}
    before = {t: (df.count(), df.toPandas()) for t, df in before.items()}

    pipeline.run(spark, pipeline_run)

    for t in tables:
        after = spark.table(pipeline_run.table(t)).drop("_ingested_at")
        assert after.count() == before[t][0], t
        cols = list(after.columns)
        pd.testing.assert_frame_equal(
            after.toPandas().sort_values(cols).reset_index(drop=True),
            before[t][1].sort_values(cols).reset_index(drop=True),
        )

    last = spark.sql(f"DESCRIBE HISTORY {pipeline_run.table('dim_customer_value')} LIMIT 1").first()
    assert last.operation == "MERGE"
    metrics = last.operationMetrics
    assert metrics["numTargetRowsUpdated"] == "0"
    assert metrics["numTargetRowsInserted"] == "0"
    assert metrics["numTargetRowsDeleted"] == "0"
