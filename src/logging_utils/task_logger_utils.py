# ----------------------
# Task-level logging (per stage or operation)
# ----------------------

import time, uuid
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.functions import current_timestamp, date_format

from io_utils.write_to_table_utils import write_task_log
from io_utils.widget_utils import _get_widget

spark = SparkSession.builder.getOrCreate()

def log_task_status(status, operation, rows=None, error=None, start_time=None, 
                   source_path=None, target_path=None, pipeline_name=None, 
                   pipeline_id=None, file_format="delta"):
    """
    Write structured task-level logs to Delta (partitioned), 
    using ADF widgets (or defaults if run manually).
    Each notebook stage (Extract, Transform, Load) logs its own status.
        Log after: Read from ADLS (rows read), Transform (rows after filtering/joins) 
        or write (rows written, target table name). 
        This helps when debugging issues to know exactly which step failed.
    
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
    # pipeline_id = pipeline_id or _get_widget("pipeline_id", str(uuid.uuid4()))
    pipeline_id = pipeline_id or _get_widget("pipeline_id", f"local_run")
    pipeline_name = pipeline_name or _get_widget("pipeline_name", "unknown_pipeline")
    run_id = _get_widget("run_id", f"local_run_id_{int(time.time())}")
    task_id = _get_widget("task_id", str(uuid.uuid4()))
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
    
    # Create log entry
    log_entry = Row(
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
        timestamp=None,  # set by current_timestamp()
        log_date=None    # derived from timestamp
    )
    
    # Create DataFrame
    log_df = (spark.createDataFrame([log_entry], schema=schema)
             .withColumn("timestamp", current_timestamp())
             .withColumn("log_date", date_format("timestamp", "yyyy-MM-dd")))
    
    # Write with simple partitioning - just environment and date
    partition_cols = ["environment", "log_date"]
    write_task_log(log_df, environment=environment, partition_cols=partition_cols, file_format=file_format)


# Context manager for automatic logging 
class TaskLogger:
    def __init__(self, operation,log_running=False,**kwargs):
        self.operation = operation
        self.log_running = log_running
        self.kwargs = kwargs
        self.start_time = None
        
    def __enter__(self):
        self.start_time = time.time()

         # Only log RUNNING status if requested
        if self.log_running:
            log_task_status(
                status="RUNNING",
                operation=self.operation,
                start_time=self.start_time,
                **self.kwargs
            )
            
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        status = "SUCCESS" if exc_type is None else "FAILED"
        
        # Log task completion with all accumulated metrics
        log_task_status(
            status=status,
            operation=self.operation,
            error=exc_val,
            start_time=self.start_time,
            **self.kwargs  # This includes any metrics added during execution
        )
        
        return False  # Don't suppress exceptions
    
    def set_metrics(self, **metrics):
        """Update metrics during task execution"""
        self.kwargs.update(metrics)
