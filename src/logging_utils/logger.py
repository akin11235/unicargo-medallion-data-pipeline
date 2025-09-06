#  Logging utility

import time, uuid
from datetime import datetime
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import current_timestamp, date_format

from io_utils.write_to_table_utils import write_pipeline_log, write_task_log

spark = SparkSession.builder.getOrCreate()

# Configurable log storage path
LOG_PATH = "abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/logs/pipeline_completion"
# MAX_RETRIES = 3
# RETRY_DELAY = 5

LOG_PATH_PIPELINE = "abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/logs/pipeline_logs"
LOG_PATH_TASK = "abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/logs/task_logs"


# -----------------------
# Pipeline Log Utility
# -----------------------
def log_pipeline_event(
    pipeline_name=None,
    catalog=None,
    run_id=None,
    status="RUNNING",
    start_time=None,
    end_time=None,
    rows_processed=0,
    message="",
    pipeline_id=None,
    file_format="delta"
):
    """
    Write structured pipeline-level logs to Delta (partitioned),
    automatically using ADF widgets (or defaults if run manually).
    """
    # --- Get identifiers ---
    pipeline_id   = pipeline_id or _get_widget("pipeline_id", str(uuid.uuid4()))
    pipeline_name = pipeline_name or _get_widget("pipeline_name", "pipeline_name")
    run_id        = run_id or _get_widget("run_id", f"local_run_{int(time.time())}")
    environment   = catalog or _get_widget("catalog", "unikargo_dev")

    start_time = start_time or datetime.now()
    end_time   = end_time or datetime.now()
    duration   = (end_time - start_time).total_seconds()

    # --- Create Row ---
    log_row = Row(
        pipeline_id=pipeline_id,
        pipeline_name=pipeline_name,
        environment=environment,
        run_id=run_id,
        status=status,
        start_time=start_time,
        end_time=end_time,
        duration=duration,
        rows_processed=rows_processed,
        message=message,
        timestamp=None
    )

    # --- Define schema ---
    log_schema = StructType([
        StructField("pipeline_id", StringType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("environment", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration", LongType(), True),
        StructField("rows_processed", LongType(), True),
        StructField("message", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])


    # --- Create Spark DataFrame with timestamp ---
    log_df = (
        spark.createDataFrame([log_row], schema=log_schema)
             .withColumn("timestamp", current_timestamp())
    )

    # --- Partition by pipeline_id + name + environment ---
    partition_cols = ["pipeline_id", "pipeline_name", "environment"]

    # --- Write using _safe_write ---
    # _safe_write(log_df, LOG_PATH_PIPELINE, partition_cols, file_format=file_format)
    write_pipeline_log(log_df, environment="dev", partition_cols=partition_cols, file_format=file_format)


# ----------------------
# Task-level logging (per stage or operation)
# ----------------------

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
        timestamp=None,  # set by current_timestamp()
        log_date=None    # derived from timestamp
    )
    
    # Create DataFrame
    log_df = (spark.createDataFrame([log_row], schema=schema)
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

# def log_task_status(status, rows=None, message="", pipeline_name=None, pipeline_id=None, file_format="delta"):
#     """
#     Write structured task-level logs to Delta (partitioned),
#     automatically using ADF widgets (or defaults if run manually).

#     Each notebook stage (Extract, Transform, Load) logs its own status.
#         Log after: Read from ADLS (rows read), Transform (rows after filtering/joins) 
#         or write (rows written, target table name). 
#         This helps when debugging issues to know exactly which step failed.
#     """

#     # --- Get pipeline/run/task identifiers ---
#     pipeline_id   = pipeline_id or _get_widget("pipeline_id", str(uuid.uuid4()))
#     pipeline_name = pipeline_name or _get_widget("pipeline_name", "pipeline_name")
#     run_id = _get_widget("run_id", f"local_run_{int(time.time())}")
#     task_id = _get_widget("task_id", "local_test")
#     environment  = _get_widget("catalog", "unikargo_dev")

#     # --- Create Row for logging ---
#     log_row = Row(
#         pipeline_id=pipeline_id,
#         pipeline_name=pipeline_name,
#         environment=environment,
#         run_id=run_id,
#         task_id=task_id,
#         status=status,
#         rows=rows,
#         message=message,
#         timestamp=None
#     )

#     # --- Define schema ---
#     log_schema = StructType([
#         StructField("pipeline_id", StringType(), True),
#         StructField("pipeline_name", StringType(), True),
#         StructField("environment", StringType(), True),
#         StructField("run_id", StringType(), True),
#         StructField("task_id", StringType(), True),
#         StructField("status", StringType(), True),
#         StructField("rows", LongType(), True),
#         StructField("message", StringType(), True),
#         StructField("timestamp", TimestampType(), True),
#     ])

#     # --- Create Spark DataFrame with timestamp ---
#     log_df = (
#         spark.createDataFrame([log_row], schema=log_schema)
#              .withColumn("timestamp", current_timestamp())
#     )

#     # Partition by key columns
#     partition_cols = ["pipeline_id", "pipeline_name", "environment", "task_id"]

#     # Append to Delta (or parquet)
#     # _safe_write(log_df, LOG_PATH_TASK, partition_cols, file_format=file_format)
#     write_task_log(log_df, environment="dev", partition_cols=partition_cols, file_format=file_format)


# -----------------------
# Helpers
# -----------------------
def new_run_id():
    """Generate a unique run_id for a pipeline execution."""
    return str(uuid.uuid4())


# def _safe_write(log_df, path, partition_cols, file_format="delta"):
#     """
#     Retry-safe write to Delta or Parquet, auto-creating the table if it doesn't exist.

#     Parameters:
#         log_df (DataFrame): Spark DataFrame to write
#         path (str): target path
#         partition_cols (list): columns to partition by
#         file_format (str): "delta" (default) or "parquet"
#     """
#     attempt = 0
#     while attempt < MAX_RETRIES:
#         try:
#             writer = log_df.write.mode("append").partitionBy(*partition_cols)

#             if file_format.lower() == "delta":
#                 # Try writing; if table doesn't exist, create it
#                 try:
#                     writer.format("delta").save(path)
#                 except AnalysisException as e:
#                     # If path doesn't exist, create Delta table
#                     if "Path does not exist" in str(e) or "Table or view not found" in str(e):
#                         writer.format("delta").option("overwriteSchema", "true").save(path)
#                     else:
#                         raise

#             elif file_format.lower() == "parquet":
#                 writer.format("parquet").save(path)
#             else:
#                 raise ValueError(f"Unsupported file format: {file_format}")

#             return  # success

#         except Exception as e:
#             attempt += 1
#             print(f"Logging attempt {attempt} failed: {e}")
#             time.sleep(RETRY_DELAY)

#     raise RuntimeError(f"Failed to write logs after {MAX_RETRIES} attempts.")

def _get_widget(name, default=""):
    """
    Safe wrapper for dbutils.widgets.get:
    returns default if dbutils or widget does not exist.
    """
    try:
        import dbutils
        return dbutils.widgets.get(name)
    except Exception:
        return default
    