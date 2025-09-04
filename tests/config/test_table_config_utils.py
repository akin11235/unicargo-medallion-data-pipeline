# import os
# import sys

# import pytest

# # Current working directory
# current_dir = os.getcwd()
# # Go up 1 levels and append 'src'
# # project_root = os.path.abspath(os.path.join(current_dir, '..', 'src'))
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
# # Add src to sys.path

# sys.path.insert(0, project_root) 
# # sys.path.append(project_root)

# from datetime import datetime
# from unittest.mock import patch
# from pyspark.sql import SparkSession

# from config.table_config_utils import get_table_config, TableConfig

# # Sample mocked TABLES_CONFIG and ENVIRONMENTS
# MOCK_TABLES_CONFIG = {
#     "airports": {
#         "bronze": "airports_bronze",
#         "silver": "airports_silver",
#         "gold": {
#             "daily_performance": "airports_gold_daily"
#         }
#     }
# }

# MOCK_ENVIRONMENTS = {
#     "dev": {
#         "catalog": "store1_dev",
#         "prefix_map": {
#             "bronze": "01_bronze",
#             "silver": "02_silver",
#             "gold": "03_gold"
#         }
#     }
# }

# def test_get_table_config_bronze():
#     with patch("config.table_config_utils.TABLES_CONFIG", MOCK_TABLES_CONFIG), \
#          patch("config.table_config_utils.ENVIRONMENTS", MOCK_ENVIRONMENTS):
        
#         cfg = get_table_config(entity="airports", layer="bronze", environment="dev")
        
#         assert isinstance(cfg, TableConfig)
#         assert cfg.catalog == "store1_dev"
#         assert cfg.schema == "01_bronze"
#         assert cfg.table == "airports_bronze"
#         assert cfg.layer == "bronze"
#         assert cfg.table_key is None
#         assert cfg.full_name == "store1_dev.01_bronze.airports_bronze"


# def test_get_table_config_silver():
#     with patch("config.table_config_utils.TABLES_CONFIG", MOCK_TABLES_CONFIG), \
#          patch("config.table_config_utils.ENVIRONMENTS", MOCK_ENVIRONMENTS):
        
#         cfg = get_table_config(entity="airports", layer="silver", environment="dev")
        
#         assert cfg.schema == "02_silver"
#         assert cfg.table == "airports_silver"
#         assert cfg.full_name == "store1_dev.02_silver.airports_silver"


# def test_get_table_config_gold():
#     with patch("config.table_config_utils.TABLES_CONFIG", MOCK_TABLES_CONFIG), \
#          patch("config.table_config_utils.ENVIRONMENTS", MOCK_ENVIRONMENTS):
        
#         cfg = get_table_config(entity="airports", layer="gold", environment="dev", table_key="daily_performance")
        
#         assert cfg.schema == "03_gold"
#         assert cfg.table == "airports_gold_daily"
#         assert cfg.table_key == "daily_performance"
#         assert cfg.full_name == "store1_dev.03_gold.airports_gold_daily"


# def test_get_table_config_missing_entity():
#     with patch("config.table_config_utils.TABLES_CONFIG", MOCK_TABLES_CONFIG), \
#          patch("config.table_config_utils.ENVIRONMENTS", MOCK_ENVIRONMENTS):
        
#         with pytest.raises(KeyError):
#             get_table_config(entity="flights", layer="bronze", environment="dev")


# def test_get_table_config_missing_table_key_for_gold():
#     with patch("config.table_config_utils.TABLES_CONFIG", MOCK_TABLES_CONFIG), \
#          patch("config.table_config_utils.ENVIRONMENTS", MOCK_ENVIRONMENTS):
        
#         with pytest.raises(ValueError):
#             get_table_config(entity="airports", layer="gold", environment="dev")
