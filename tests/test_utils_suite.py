import os
import sys
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

# --------------------------
# Add src folder to sys.path
# --------------------------
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


# --------------------------
# Imports
# --------------------------
# from pyspark.sql import SparkSession
from io_utils.write_to_table_utils import save_to_table
# from config.table_config_utils import get_table_config, TableConfig

# -----------------------------
# save_to_table Tests
# -----------------------------
def test_save_to_table_overwrite(sample_df, mock_table_config):
    # Mock DataFrameWriter
    mock_writer = MagicMock()

    # Chain mode() and option() to return the same writer
    mock_writer.mode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer

    # Patch the `write` property of sample_df
    with patch.object(type(sample_df), "write", new_callable=PropertyMock) as mock_write:
        mock_write.return_value = mock_writer
        result = save_to_table(
            df=sample_df,
            entity="airports",
            layer="bronze",
            environment="dev",
            table_key=None,
            mode="overwrite",
            overwrite_schema=True
        )

    # Assertions
    mock_writer.mode.assert_called_once_with("overwrite")
    mock_writer.option.assert_called_once_with("overwriteSchema", "true")
    mock_writer.saveAsTable.assert_called_once_with("dev.bronze.airports")
    assert result.full_name == "dev.bronze.airports"

def test_save_to_table_no_overwrite(sample_df, mock_table_config):
    # Mock DataFrameWriter
    mock_writer = MagicMock()

    # Chain mode() and option() to return the same writer
    mock_writer.mode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer

    # Patch the `write` property of sample_df
    with patch.object(type(sample_df), "write", new_callable=PropertyMock) as mock_write:
        mock_write.return_value = mock_writer

        # Call the function with overwrite_schema=False
        result = save_to_table(
            df=sample_df,
            entity="airports",
            layer="bronze",
            environment="dev",
            table_key=None,
            mode="append",
            overwrite_schema=False
        )

    # Assertions
    mock_writer.mode.assert_called_once_with("append")
    # option() should NOT be called
    mock_writer.option.assert_not_called()
    mock_writer.saveAsTable.assert_called_once_with("dev.bronze.airports")
    assert result.full_name == "dev.bronze.airports"


# -----------------------------
# get_table_config Tests (fixture style)
# -----------------------------
def test_get_table_config_bronze(patch_table_config):
    cfg = patch_table_config.get_table_config(entity="airports", layer="bronze", environment="dev")
    assert cfg.schema == "01_bronze"
    assert cfg.table == "airports_bronze"
    assert cfg.full_name == "store1_dev.01_bronze.airports_bronze"


def test_get_table_config_silver(patch_table_config):
    cfg = patch_table_config.get_table_config(entity="airports", layer="silver", environment="dev")
    assert cfg.schema == "02_silver"
    assert cfg.table == "airports_silver"
    assert cfg.full_name == "store1_dev.02_silver.airports_silver"


def test_get_table_config_gold(patch_table_config):
    cfg = patch_table_config.get_table_config(entity="airports", layer="gold", environment="dev", table_key="daily_performance")
    assert cfg.schema == "03_gold"
    assert cfg.table == "airports_gold_daily"
    assert cfg.table_key == "daily_performance"
    assert cfg.full_name == "store1_dev.03_gold.airports_gold_daily"


def test_get_table_config_missing_entity(patch_table_config):
    with pytest.raises(KeyError):
        patch_table_config.get_table_config(entity="flights", layer="bronze", environment="dev")


def test_get_table_config_missing_table_key_for_gold(patch_table_config):
    with pytest.raises(ValueError):
        patch_table_config.get_table_config(entity="airports", layer="gold", environment="dev")