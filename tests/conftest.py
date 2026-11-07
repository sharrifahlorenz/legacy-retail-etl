import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(ROOT, "data", "raw")
EXPECTED_DIR = os.path.join(ROOT, "data", "expected")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from databricks_pipeline.spark import get_spark

    session = get_spark(local=True, warehouse_dir=str(tmp_path_factory.mktemp("warehouse")))
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
