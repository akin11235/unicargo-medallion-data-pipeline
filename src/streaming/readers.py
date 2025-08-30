"""
Streaming read operations for structured streaming
"""

from pyspark.sql import DataFrame
from typing import Optional
import logging

logger = logging.getLogger(__name__)

def read_streaming_table(
    spark,
    catalog_name: str, 
    schema_name: str, 
    table_name: str,
    format: str = "delta",
    options: Optional[dict] = None
) -> DataFrame:
    """
    Reads a streaming table from a specified catalog/schema/table using Structured Streaming.

    Parameters:
    - spark: SparkSession instance
    - catalog_name: The catalog in Unity Catalog
    - schema_name: The schema (database) containing the table
    - table_name: The name of the table to stream from
    - format: Table format (default: "delta")
    - options: Additional read options

    Returns:
    - A streaming DataFrame
    """
    table_full_name = f"{catalog_name}.{schema_name}.{table_name}"
    logger.info(f"Reading streaming table: {table_full_name}")
    
    try:
        stream_reader = spark.readStream.format(format)
        
        # Apply additional options if provided
        if options:
            for key, value in options.items():
                stream_reader = stream_reader.option(key, value)
        
        df_stream = stream_reader.table(table_full_name)
        
        logger.info(f"Successfully initialized streaming DataFrame for {table_full_name}")
        return df_stream
        
    except Exception as e:
        logger.error(f"Failed to read streaming table {table_full_name}: {str(e)}")
        raise

def read_streaming_path(
    spark,
    path: str,
    format: str = "delta",
    schema = None,
    options: Optional[dict] = None
) -> DataFrame:
    """
    Reads streaming data from a file path.
    
    Parameters:
    - spark: SparkSession instance
    - path: Path to read from
    - format: File format (default: "delta")
    - schema: Optional schema to apply
    - options: Additional read options
    
    Returns:
    - A streaming DataFrame
    """
    logger.info(f"Reading streaming data from path: {path}")
    
    try:
        stream_reader = spark.readStream.format(format)
        
        # Apply schema if provided
        if schema:
            stream_reader = stream_reader.schema(schema)
        
        # Apply additional options if provided
        if options:
            for key, value in options.items():
                stream_reader = stream_reader.option(key, value)
        
        df_stream = stream_reader.load(path)
        
        logger.info(f"Successfully initialized streaming DataFrame from {path}")
        return df_stream
        
    except Exception as e:
        logger.error(f"Failed to read streaming data from {path}: {str(e)}")
        raise