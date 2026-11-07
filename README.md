# legacy-retail-etl

> **Migration in progress:** the nightly batch is being moved to a Databricks Lakehouse pipeline
> (PySpark + Delta Lake + Unity Catalog, deployed with Databricks Asset Bundles) in
> [`databricks_pipeline/`](databricks_pipeline/). See [MIGRATION.md](MIGRATION.md) for the
> architecture, rule mapping, parity results and deployment. `legacy/` stays in place until cut-over.

Nightly retail analytics batch for a mid-market omnichannel retailer. Written in
2019 for a single on-prem batch server (`etl-batch-01.corp.local`) and still running there via cron.

## What it does

```
data/raw/*.csv  ──►  extract.py  ──►  transform.py (pandas, in-memory)  ──►  load.py  ──►  SQLite + CSV on a shared drive
 (POS + ecom drops)                                                                 fct_order_lines
                                                                                    agg_daily_sales
                                                                                    dim_customer_value
```

| Output table | Grain | Used by |
|---|---|---|
| `fct_order_lines` | one row per order line, cleaned and enriched with region, category, cost and margin | finance reconciliation |
| `agg_daily_sales` | date × region × category | exec dashboard |
| `dim_customer_value` | one row per customer, lifetime value tier (VIP / MID / STANDARD) | CRM / marketing |

Business rules worth knowing:
- Raw files arrive with **exact-duplicate lines** (double-delivered files), so these are dropped.
- `order_date` arrives as `YYYY-MM-DD` **or** `MM/DD/YYYY` (legacy POS). Unparseable rows are dropped.
- Negative `quantity` = a return. Returns stay in revenue (they net it down).
- Blank `discount_pct` = 0.
- Orders with a `customer_id` not in the customer master are dropped (inner join, ticket DATA-212).
- Tier thresholds are set in `legacy/config.py`.

## Known problems

- Row-by-row `iterrows()` and per-customer Python loops make it slow, and it OOMs on peak-season volume.
- It truncates and reloads every night: no history, no time travel, no lineage.
- It has a single point of failure, with no orchestration, retries or alerting beyond cron + log file.
- Analysts pull CSVs off a shared drive with no governance or access control.

## Run it locally

```bash
pip install -r requirements.txt      # includes pyspark + delta-spark; Spark needs Java 11 or 17
python scripts/generate_data.py      # regenerate deterministic raw data (seed=42)
python -m legacy.run_nightly         # legacy job, writes data/output/
pytest -q                            # legacy tests + Spark silver unit tests + parity tests
```

Run the new Lakehouse pipeline locally (Delta tables under `spark-warehouse/`):

```bash
python -m databricks_pipeline.main all --local --catalog spark_catalog --schema retail \
    --ingest-mode batch --source-path data/raw
```

## Databricks pipeline

```
Volume (raw CSV) ─Auto Loader─► bronze_* ─► silver_* ─► fct_order_lines / agg_daily_sales / dim_customer_value
                                  (strings + metadata)  (rules)  (Delta, <catalog>.<schema>, default main.retail)
```

| Path | Purpose |
|---|---|
| `databricks_pipeline/` | bronze / silver / gold PySpark modules, config, CLI entry point `retail_pipeline` |
| `databricks.yml`, `resources/` | Asset Bundle: `dev` / `prod` targets, nightly 02:00 Job (bronze → silver → gold) |
| `tests/test_silver.py` | unit tests for every ported business rule |
| `tests/test_parity.py` | runs the pipeline on local Spark and checks it against `data/expected/` |
| `.github/workflows/ci.yml` | pytest (with Java) + `databricks bundle validate` when `DATABRICKS_HOST` is set |

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

`data/expected/` holds golden snapshots of `agg_daily_sales` and `dim_customer_value`
produced by the legacy job. Any migration must reproduce them.
