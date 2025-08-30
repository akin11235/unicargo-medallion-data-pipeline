"""
Data cleaning and quality operations
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

def add_ingestion_timestamp(input_df: DataFrame, column_name: str = "ingestion_date") -> DataFrame:
    """
    Adds a current timestamp column to the DataFrame.
    
    Parameters:
    - input_df: Input DataFrame
    - column_name: Name for the timestamp column
    
    Returns:
    - DataFrame with added timestamp column
    """
    logger.info(f"Adding ingestion timestamp column: {column_name}")
    output_df = input_df.withColumn(column_name, current_timestamp())
    logger.info("Ingestion timestamp added successfully")
    return output_df

def drop_columns(input_df: DataFrame, columns_to_drop: List[str]) -> DataFrame:
    """
    Drops specified columns from DataFrame.
    
    Parameters:
    - input_df: Input DataFrame
    - columns_to_drop: List of column names to drop
    
    Returns:
    - DataFrame with specified columns removed
    """
    if not columns_to_drop:
        logger.info("No columns specified for dropping")
        return input_df
    
    # Filter out columns that don't exist
    existing_columns = input_df.columns
    valid_columns = [col for col in columns_to_drop if col in existing_columns]
    invalid_columns = [col for col in columns_to_drop if col not in existing_columns]
    
    if invalid_columns:
        logger.warning(f"Columns not found in DataFrame: {invalid_columns}")
    
    if not valid_columns:
        logger.info("No valid columns to drop")
        return input_df
    
    logger.info(f"Dropping columns: {valid_columns}")
    df_result = input_df.drop(*valid_columns)
    logger.info(f"Successfully dropped {len(valid_columns)} columns")
    return df_result

def remove_duplicates(input_df: DataFrame, subset: Optional[List[str]] = None) -> DataFrame:
    """
    Removes duplicate records from DataFrame.
    
    Parameters:
    - input_df: Input DataFrame
    - subset: Optional list of columns to consider for duplicates
    
    Returns:
    - DataFrame with duplicates removed
    """
    logger.info("Removing duplicates...")
    
    if subset:
        logger.info(f"Checking duplicates based on columns: {subset}")
        df_result = input_df.dropDuplicates(subset)
    else:
        logger.info("Checking duplicates based on all columns")
        df_result = input_df.dropDuplicates()
    
    original_count = input_df.count() if logger.isEnabledFor(logging.INFO) else 0
    final_count = df_result.count() if logger.isEnabledFor(logging.INFO) else 0
    
    if logger.isEnabledFor(logging.INFO):
        duplicates_removed = original_count - final_count
        logger.info(f"Removed {duplicates_removed} duplicate records")
    
    return df_result

def handle_null_values(
    input_df: DataFrame,
    string_fill_value: str = "Unknown",
    numeric_fill_value: int = 0,
    custom_fill_values: Optional[Dict[str, Any]] = None
) -> DataFrame:
    """
    Handles null values in DataFrame with configurable fill strategies.
    
    Parameters:
    - input_df: Input DataFrame
    - string_fill_value: Value to fill null strings
    - numeric_fill_value: Value to fill null numbers
    - custom_fill_values: Dictionary of column-specific fill values
    
    Returns:
    - DataFrame with null values handled
    """
    logger.info("Handling null values...")
    
    df_result = input_df
    
    # Apply custom fill values first
    if custom_fill_values:
        logger.info(f"Applying custom fill values: {custom_fill_values}")
        for column, fill_value in custom_fill_values.items():
            if column in df_result.columns:
                df_result = df_result.fillna(fill_value, subset=[column])
    
    # Get all columns
    all_columns = df_result.columns
    
    # Fill string columns
    logger.info(f"Filling null string values with '{string_fill_value}'")
    df_result = df_result.fillna(string_fill_value, subset=all_columns)
    
    # Fill numeric columns
    logger.info(f"Filling null numeric values with {numeric_fill_value}")
    df_result = df_result.fillna(numeric_fill_value, subset=all_columns)
    
    logger.info("Null values handled successfully")
    return df_result

def clean_dataframe(
    input_df: DataFrame,
    operations: List[str] = ["add_timestamp", "drop_columns", "remove_duplicates", "handle_nulls"],
    config: Optional[Dict[str, Any]] = None
) -> DataFrame:
    """
    Applies multiple cleaning operations in sequence.
    
    Parameters:
    - input_df: Input DataFrame
    - operations: List of operations to apply
    - config: Configuration dictionary for operations
    
    Returns:
    - Cleaned DataFrame
    """
    config = config or {}
    df_result = input_df
    
    logger.info(f"Starting data cleaning pipeline with operations: {operations}")
    
    for operation in operations:
        if operation == "add_timestamp":
            timestamp_col = config.get("timestamp_column", "ingestion_date")
            df_result = add_ingestion_timestamp(df_result, timestamp_col)
        
        elif operation == "drop_columns":
            columns_to_drop = config.get("columns_to_drop", [])
            df_result = drop_columns(df_result, columns_to_drop)
        
        elif operation == "remove_duplicates":
            subset = config.get("duplicate_subset")
            df_result = remove_duplicates(df_result, subset)
        
        elif operation == "handle_nulls":
            string_fill = config.get("string_fill_value", "Unknown")
            numeric_fill = config.get("numeric_fill_value", 0)
            custom_fill = config.get("custom_fill_values")
            df_result = handle_null_values(df_result, string_fill, numeric_fill, custom_fill)
        
        else:
            logger.warning(f"Unknown operation: {operation}")
    
    logger.info("Data cleaning pipeline completed")
    return df_result