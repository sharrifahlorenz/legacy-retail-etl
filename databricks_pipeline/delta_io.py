"""Idempotent Delta writers."""
import logging
import uuid

log = logging.getLogger(__name__)


def ensure_schema(spark, cfg):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")


def overwrite_table(df, table):
    """Full replace in a single Delta commit; the previous version stays available via time travel."""
    log.info("overwriting %s", table)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )


def merge_table(spark, df, table, keys):
    """Upsert ``df`` into ``table`` on ``keys`` and delete target rows missing from ``df``.

    Rows whose values did not change are left untouched, so a rerun on the same input commits
    a no-op MERGE and the table ends up identical to the source either way.
    """
    df.limit(0).write.format("delta").mode("ignore").saveAsTable(table)  # create if missing

    log.info("merging into %s on %s", table, keys)
    source = f"_merge_src_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(source)
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    changed = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in df.columns if c not in keys)
    try:
        spark.sql(f"""
            MERGE INTO {table} AS t
            USING {source} AS s
            ON {on}
            WHEN MATCHED AND ({changed}) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)
    finally:
        spark.catalog.dropTempView(source)
