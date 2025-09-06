import time
import uuid
import traceback
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.functions import current_timestamp, date_format

def log_task_status(status, operation, rows=None, error=None, start_time=None, 
                   source_path=None, target_path=None, pipeline_name=None, 
                   pipeline_id=None, file_format="delta"):
    """
    Simple, focused task logging with essential metrics only.
    
    Args:
        status: SUCCESS, FAILED, RUNNING
        operation: What operation was performed (e.g. "read_flights", "transform_data")  
        rows: Number of rows processed
        error: Exception object if failed
        start_time: Task start time (time.time())
        source_path: Source data location
        target_path: Target data location
    """
    
    # Calculate execution time
    execution_time_ms = None
    if start_time:
        execution_time_ms = int((time.time() - start_time) * 1000)
    
    # Handle error details
    error_type = None
    error_message = None
    if error:
        error_type = type(error).__name__
        error_message = str(error)[:200]  # Keep it short
    
    # Get pipeline context
    pipeline_id = pipeline_id or _get_widget("pipeline_id", str(uuid.uuid4()))
    pipeline_name = pipeline_name or _get_widget("pipeline_name", "unknown_pipeline")
    run_id = _get_widget("run_id", f"local_run_{int(time.time())}")
    task_id = _get_widget("task_id", "local_task")
    environment = _get_widget("catalog", "dev")
    
    # Simple schema - only essential fields
    schema = StructType([
        StructField("pipeline_id", StringType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("environment", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField("task_id", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("status", StringType(), True),
        StructField("rows", LongType(), True),
        StructField("execution_time_ms", LongType(), True),
        StructField("source_path", StringType(), True),
        StructField("target_path", StringType(), True),
        StructField("error_type", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("log_date", StringType(), True),  # Partition key
    ])
    
    # Create log row
    log_row = Row(
        pipeline_id=pipeline_id,
        pipeline_name=pipeline_name,
        environment=environment,
        run_id=run_id,
        task_id=task_id,
        operation=operation,
        status=status,
        rows=rows,
        execution_time_ms=execution_time_ms,
        source_path=source_path,
        target_path=target_path,
        error_type=error_type,
        error_message=error_message,
        timestamp=None,  # Will be set by current_timestamp()
        log_date=None    # Will be derived from timestamp
    )
    
    # Create DataFrame
    log_df = (spark.createDataFrame([log_row], schema=schema)
             .withColumn("timestamp", current_timestamp())
             .withColumn("log_date", date_format("timestamp", "yyyy-MM-dd")))
    
    # Write with simple partitioning - just environment and date
    partition_cols = ["environment", "log_date"]
    write_task_log(log_df, environment=environment, partition_cols=partition_cols, file_format=file_format)



def _get_widget(name, default):
    """Get widget value or return default"""
    try:
        return dbutils.widgets.get(name)
    except:
        return default