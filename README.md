# legacy-retail-etl

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
pip install -r requirements.txt
python scripts/generate_data.py      # regenerate deterministic raw data (seed=42)
python -m legacy.run_nightly         # writes data/output/
pytest -q
```

`data/expected/` holds golden snapshots of `agg_daily_sales` and `dim_customer_value`
produced by the legacy job. Any migration must reproduce them.
