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
# from unittest.mock import MagicMock, patch, PropertyMock
# from pyspark.sql import SparkSession

# from io_utils.write_to_table_utils import save_to_table



# def test_save_to_table_calls_saveAsTable(sample_df, mock_table_config):
#     # Mock DataFrameWriter
#     mock_writer = MagicMock()
    
#     # Chain mode() and option() to return the same writer
#     mock_writer.mode.return_value = mock_writer
#     mock_writer.option.return_value = mock_writer
    
#     # Patch the `write` property of sample_df
#     with patch.object(type(sample_df), "write", new_callable=PropertyMock) as mock_write:
#         mock_write.return_value = mock_writer
        
#         # Call the function
#         result = save_to_table(
#             df=sample_df,
#             entity="airports",
#             layer="bronze",
#             environment="dev",
#             table_key=None,
#             mode="overwrite",
#             overwrite_schema=True
#         )

#     # Assertions
#     mock_writer.mode.assert_called_once_with("overwrite")
#     mock_writer.option.assert_called_once_with("overwriteSchema", "true")
#     mock_writer.saveAsTable.assert_called_once_with("dev.bronze.airports")
#     assert result.full_name == "dev.bronze.airports"



# def test_save_to_table_no_overwrite_schema(sample_df, mock_table_config):
#     # Mock DataFrameWriter
#     mock_writer = MagicMock()
    
#     # Chain mode() and option() to return the same writer
#     mock_writer.mode.return_value = mock_writer
#     mock_writer.option.return_value = mock_writer
    
#     # Patch the `write` property of sample_df
#     with patch.object(type(sample_df), "write", new_callable=PropertyMock) as mock_write:
#         mock_write.return_value = mock_writer
        
#         # Call the function with overwrite_schema=False
#         result = save_to_table(
#             df=sample_df,
#             entity="airports",
#             layer="bronze",
#             environment="dev",
#             table_key=None,
#             mode="append",
#             overwrite_schema=False
#         )

#     # Assertions
#     mock_writer.mode.assert_called_once_with("append")
#     # option() should NOT be called
#     mock_writer.option.assert_not_called()
#     mock_writer.saveAsTable.assert_called_once_with("dev.bronze.airports")
#     assert result.full_name == "dev.bronze.airports"
