"""CLI / Databricks Job entry point.

    retail_pipeline {bronze,silver,gold,all} [--catalog main --schema retail ...]

Local run against ``data/raw`` (writes Delta tables under ``spark-warehouse/``)::

    python -m databricks_pipeline.main all --local --catalog spark_catalog --ingest-mode batch \
        --source-path data/raw
"""
import argparse
import logging
import sys
import time

from databricks_pipeline import bronze, gold, silver
from databricks_pipeline.config import PipelineConfig

LAYERS = {"bronze": bronze.run, "silver": silver.run, "gold": gold.run}

log = logging.getLogger("databricks_pipeline")


def run(spark, cfg, layers=tuple(LAYERS)):
    for layer in layers:
        t0 = time.time()
        LAYERS[layer](spark, cfg)
        log.info("%s finished in %.1fs", layer, time.time() - t0)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="retail_pipeline", description=__doc__.splitlines()[0])
    parser.add_argument("layer", choices=[*LAYERS, "all"])
    parser.add_argument("--local", action="store_true", help="start a local Spark session with Delta")
    parser.add_argument("--warehouse-dir", default=None, help="local spark.sql.warehouse.dir")
    PipelineConfig.add_arguments(parser)
    return parser.parse_args(argv)


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args(sys.argv[1:] if argv is None else argv)
    cfg = PipelineConfig.from_args(args)
    log.info("config: %s", cfg)

    from databricks_pipeline.spark import get_spark

    spark = get_spark(local=args.local, warehouse_dir=args.warehouse_dir)
    run(spark, cfg, tuple(LAYERS) if args.layer == "all" else (args.layer,))


if __name__ == "__main__":
    main()
