"""Pipeline configuration.

Every value can be set from the command line (used by the Databricks Job tasks) or from a
``RETAIL_*`` environment variable; the defaults below match the legacy job (``legacy/config.py``).
"""
import argparse
import os
from dataclasses import dataclass, fields

INGEST_MODES = ("autoloader", "batch")

# Columns of each raw CSV extract, in file order. Bronze keeps them all as strings.
SOURCES = {
    "orders": [
        "order_id", "line_no", "customer_id", "product_id", "quantity",
        "unit_price", "discount_pct", "order_date", "channel",
    ],
    "customers": ["customer_id", "first_name", "last_name", "email", "region", "signup_date"],
    "products": ["product_id", "sku", "product_name", "category", "unit_cost", "list_price"],
}


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str = "main"
    schema: str = "retail"
    # Unity Catalog Volume holding the raw CSV drops (or a local directory in batch mode).
    source_path: str = "/Volumes/main/retail/raw"
    # Auto Loader checkpoints and schema tracking (one sub-directory per source).
    checkpoint_path: str = "/Volumes/main/retail/_checkpoints"
    # "autoloader" = incremental cloudFiles stream (Databricks); "batch" = spark.read.csv (local / tests).
    ingest_mode: str = "autoloader"
    # Lifetime net revenue thresholds for the customer value tiers (legacy HIGH/MID_VALUE_THRESHOLD).
    vip_threshold: float = 40000.0
    mid_threshold: float = 25000.0

    def __post_init__(self):
        if self.ingest_mode not in INGEST_MODES:
            raise ValueError(f"ingest_mode must be one of {INGEST_MODES}, got {self.ingest_mode!r}")
        if self.mid_threshold > self.vip_threshold:
            raise ValueError("mid_threshold must not exceed vip_threshold")

    def table(self, name):
        return f"{self.catalog}.{self.schema}.{name}"

    @classmethod
    def from_env(cls, **overrides):
        values = {}
        for f in fields(cls):
            env = os.environ.get(f"RETAIL_{f.name.upper()}")
            if env is not None:
                values[f.name] = f.type(env) if f.type is float else env
        values.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**values)

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser):
        for f in fields(cls):
            parser.add_argument(
                "--" + f.name.replace("_", "-"),
                dest=f.name,
                type=float if f.type is float else str,
                choices=INGEST_MODES if f.name == "ingest_mode" else None,
                default=None,
                help=f"default: $RETAIL_{f.name.upper()} or {f.default!r}",
            )

    @classmethod
    def from_args(cls, args: argparse.Namespace):
        return cls.from_env(**{f.name: getattr(args, f.name, None) for f in fields(cls)})
