"""
Streaming write operations for structured streaming
"""

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from typing import Optional, Callable
import logging

logger = logging.getLogger(__name__)

def write_stream_to_table(
    streaming_df: DataFrame,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    checkpoint_location: str,
    output_mode: str = "append",
    trigger_config: Optional[dict] = None,
    wait_for_completion: bool = True,
    format: str = "delta"
) -> StreamingQuery:
    """
    Writes a streaming DataFrame to a Delta table using Structured Streaming.

    Parameters:
    - streaming_df: The streaming DataFrame to write
    - catalog_name: The catalog in Unity Catalog
    - schema_name: The schema (database) containing the table
    - table_name: The name of the table to write to
    - checkpoint_location: Path for Spark to store checkpoint data
    - output_mode: Output mode ("append", "complete", "update")
    - trigger_config: Trigger configuration (e.g., {"availableNow": True})
    - wait_for_completion: Whether to wait for completion
    - format: Output format (default: "delta")

    Returns:
    - A streaming query object
    """
    table_full_name = f"{catalog_name}.{schema_name}.{table_name}"
    query_name = f"{catalog_name}_{schema_name}_{table_name}_writestream"
    
    logger.info(f"Starting stream write to table: {table_full_name}")

    try:
        stream_writer = streaming_df.writeStream \
            .format(format) \
            .option("checkpointLocation", checkpoint_location) \
            .outputMode(output_mode) \
            .queryName(query_name)
        
        # Configure trigger
        if trigger_config:
            if "availableNow" in trigger_config:
                stream_writer = stream_writer.trigger(availableNow=trigger_config["availableNow"])
            elif "processingTime" in trigger_config:
                stream_writer = stream_writer.trigger(processingTime=trigger_config["processingTime"])
            elif "once" in trigger_config:
                stream_writer = stream_writer.trigger(once=trigger_config["once"])
        else:
            # Default to availableNow=True
            stream_writer = stream_writer.trigger(availableNow=True)
        
        query = stream_writer.toTable(table_full_name)

        if wait_for_completion:
            query.awaitTermination()
            logger.info(f"Stream write to {table_full_name} completed successfully")
        else:
            logger.info(f"Stream to {table_full_name} started, running in background")

        return query

    except Exception as e:
        logger.error(f"Failed to write stream to {table_full_name}: {str(e)}")
        raise

def write_stream_with_merge(
    streaming_df: DataFrame,
    target_table: str,
    checkpoint_location: str,
    merge_keys: list,
    merge_condition: Optional[str] = None,
    when_matched_action: str = "UPDATE SET *",
    when_not_matched_action: str = "INSERT *"
) -> StreamingQuery:
    """
    Writes a streaming DataFrame using MERGE operation.
    
    Parameters:
    - streaming_df: The streaming DataFrame to write
    - target_table: Full table name (catalog.schema.table)
    - checkpoint_location: Checkpoint location
    - merge_keys: List of keys to merge on
    - merge_condition: Custom merge condition (optional)
    - when_matched_action: Action when records match
    - when_not_matched_action: Action when records don't match
    
    Returns:
    - A streaming query object
    """
    def merge_function(batch_df, batch_id):
        batch_df.createOrReplaceTempView("updates")
        
        # Build merge condition
        if merge_condition:
            condition = merge_condition
        else:
            condition = ' AND '.join([f'target.{key} = source.{key}' for key in merge_keys])
        
        merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING updates AS source
            ON {condition}
            WHEN MATCHED THEN {when_matched_action}
            WHEN NOT MATCHED THEN {when_not_matched_action}
        """
        
        logger.info(f"Executing merge for batch {batch_id}")
        streaming_df.sparkSession.sql(merge_sql)
    
    logger.info(f"Starting merge stream to {target_table}")
    
    query = streaming_df.writeStream \
        .option("checkpointLocation", checkpoint_location) \
        .outputMode("update") \
        .queryName(f"{target_table}_merge_stream") \
        .trigger(availableNow=True) \
        .foreachBatch(merge_function) \
        .start()
    
    return query