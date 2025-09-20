import sys
import os
from pathlib import Path

import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession

try:
    # Databricks Runtime provides this
    from databricks.connect import DatabricksSession
except ImportError:
    DatabricksSession = None

# Add src folder to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # go up from tests/ to project root
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import table_config_utils
# -----------------------------
# PySpark Fixtures
# -----------------------------
# @pytest.fixture(scope="session")
# def session():
#     """Provides either a local SparkSession or a DatabricksSession."""
#     if DatabricksSession:
#         # running in Databricks
#         spark = (DatabricksSession.builder()
#                  .remote("your-databricks-workspace-url")
#                  .create())
#     else:
#         # local PySpark
#         spark = (SparkSession.builder
#                  .master("local[2]")
#                  .appName("pytest-spark")
#                  .getOrCreate())

#     yield spark
#     spark.stop()

# @pytest.fixture(scope="session")
# def session():
#     """Use DatabricksSession if available, else fallback to local SparkSession."""
#     if DatabricksSession and os.getenv("DATABRICKS_HOST"):
#         spark = (
#             DatabricksSession.builder.remote(
#                 os.getenv("DATABRICKS_HOST")
#             )
#             .token(os.getenv("DATABRICKS_TOKEN"))
#             .cluster_id(os.getenv("DATABRICKS_CLUSTER_ID"))
#             .create()
#         )
#     else:
#         spark = (
#             SparkSession.builder.master("local[2]")
#             .appName("pytest-spark")
#             .getOrCreate()
#         )

#     yield spark
#     spark.stop()

@pytest.fixture(scope="session")
def spark():
    """
    Provides a Spark session.
    Tries DatabricksSession first, falls back to local SparkSession.
    """
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
    except ImportError:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder \
                .master("local[1]") \
                .appName("pytest-spark") \
                .getOrCreate()
        except ImportError:
            raise ImportError("Neither Databricks Session nor Spark Session is available")
    yield spark
    spark.stop()

@pytest.fixture
def sample_df(spark):
    data = [("LAX", "Los Angeles"), ("JFK", "New York")]
    columns = ["code", "name"]
    return spark.createDataFrame(data, columns)

@pytest.fixture
def mock_table_config(monkeypatch):
    mock_config = MagicMock()
    mock_config.full_name = "dev.bronze.airports"

    def mock_get_table_config(**kwargs):
        return mock_config

    monkeypatch.setattr(
        "io_utils.write_to_table_utils.get_table_config",
        mock_get_table_config
    )
    return mock_config

# -----------------------------
# Shared Mock Configs
# -----------------------------
@pytest.fixture
def mock_tables_config():
    return {
        "airports": {
            "bronze": "airports_bronze",
            "silver": "airports_silver",
            "gold": {"daily_performance": "airports_gold_daily"}
        }
    }

@pytest.fixture
def mock_environments():
    return {
        "dev": {
            "catalog": "store1_dev",
            "prefix_map": {
                "bronze": "01_bronze",
                "silver": "02_silver",
                "gold": "03_gold"
            }
        }
    }

@pytest.fixture
def patch_table_config(monkeypatch, mock_tables_config, mock_environments):
    from config import table_config_utils
    monkeypatch.setattr(table_config_utils, "TABLES_CONFIG", mock_tables_config)
    monkeypatch.setattr(table_config_utils, "ENVIRONMENTS_CONFIG", mock_environments)
    return table_config_utils