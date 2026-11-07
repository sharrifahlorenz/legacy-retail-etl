"""Bronze: land the raw CSV extracts as-is (all strings) plus ingestion metadata."""
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from databricks_pipeline.config import SOURCES
from databricks_pipeline.delta_io import ensure_schema, overwrite_table

log = logging.getLogger(__name__)


def bronze_table(cfg, source):
    return cfg.table(f"bronze_{source}")


def source_schema(source):
    return StructType([StructField(c, StringType(), True) for c in SOURCES[source]])


def _csv_options(source):
    # Files land as e.g. orders.csv or orders_20240101.csv; one glob per source.
    return {"header": "true", "pathGlobFilter": f"{source}*.csv"}


def with_metadata(df):
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_file", F.col("_metadata.file_path")
    )


def read_batch(spark, cfg, source):
    return with_metadata(
        spark.read.options(**_csv_options(source))
        .option("enforceSchema", "false")  # fail if the header does not match the expected columns
        .schema(source_schema(source))
        .csv(cfg.source_path)
    )


def ingest_autoloader(spark, cfg, source):
    """Incrementally append new files with Auto Loader; the checkpoint makes reruns exactly-once."""
    checkpoint = f"{cfg.checkpoint_path.rstrip('/')}/{source}"
    stream = with_metadata(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint}/_schema")
        .option("rescuedDataColumn", "_rescued_data")
        .options(**_csv_options(source))
        .schema(source_schema(source))
        .load(cfg.source_path)
    )
    query = (
        stream.writeStream.option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(bronze_table(cfg, source))
    )
    query.awaitTermination()


def run(spark, cfg):
    ensure_schema(spark, cfg)
    for source in SOURCES:
        log.info("bronze %s <- %s (%s)", source, cfg.source_path, cfg.ingest_mode)
        if cfg.ingest_mode == "autoloader":
            ingest_autoloader(spark, cfg, source)
        else:
            overwrite_table(read_batch(spark, cfg, source), bronze_table(cfg, source))
