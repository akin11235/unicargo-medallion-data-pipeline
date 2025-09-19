# ----------------------
# Task-level logging (per stage or operation)
# ----------------------

import time, uuid
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import IntegerType, MapType, StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.functions import current_timestamp, date_format

from io_utils.write_to_table_utils import write_task_log
from io_utils.widget_utils import _get_widget, _get_param
from delta import configure_spark_with_delta_pip

spark = SparkSession.builder.getOrCreate()

# Global tracker for step order per notebook run
if "_STEP_COUNTER" not in globals():
    _STEP_COUNTER = {}

# def log_task_status(status, operation, rows=None, error=None, start_time=None, 
#                    source_path=None, target_path=None, pipeline_name=None, 
#                    pipeline_id=None, file_format="delta"):
# def log_task_status(
#     status,
#     operation,
#     rows=None,
#     error=None,
#     start_time=None,
#     source_path=None,
#     target_path=None,
#     pipeline_name=None,
#     pipeline_id=None,
#     parent_task_id=None,
#     attempt_number=None,
#     tags=None,
#     etl_metrics=None,
#     file_format="delta"
# ):
def log_task_status(
    status,
    operation,
    rows=None,
    error=None,
    start_time=None,
    source_path=None,
    target_path=None,
    pipeline_name=None,
    pipeline_id=None,
    run_id=None,
    run_name=None,
    environment=None,
    task_id=None,
    step_index=None,
    step_type=None,
    parent_task_id=None,
    attempt_number=1,
    worker_node=None,
    executor_id=None,
    tags=None,
    etl_metrics=None,
    file_format="delta",  # <--- default added
    **kwargs
):
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

    global _STEP_COUNTER
    
    # Calculate execution time
    # execution_time_ms = None
    # if start_time:
    #     execution_time_ms = int((time.time() - start_time) * 1000)
    execution_time_ms = int((time.time() - start_time) * 1000) if start_time else None
    
    # Handle error details
    # error_type = None
    # error_message = None
    # if error:
    #     error_type = type(error).__name__
    #     error_message = str(error)[:200]  # Keep it short
    error_type = type(error).__name__ if error else None
    error_message = str(error)[:200] if error else None
    
    # Get pipeline context
    # pipeline_id = pipeline_id or _get_widget("pipeline_id", f"local_run")
    # pipeline_name = pipeline_name or _get_widget("pipeline_name", "unknown_pipeline")
    # run_id = _get_widget("run_id", f"local_run_id_{int(time.time())}")
    # run_name = _get_widget("run_name", f"local_run_name_{uuid.uuid4()}")
    # environment = _get_widget("ENV", "dev")
    # task_id = task_id or _get_widget("task_id", f"local_task_id_{str(uuid.uuid4())}")

    pipeline_id   = _get_param(1, "pipeline_id", f"local_run", spark)
    pipeline_name = _get_param(2, "pipeline_name", "unknown_pipeline", spark)
    run_id        = _get_param(3, "run_id", f"local_run_id_{int(time.time())}", spark)
    run_name      = _get_param(4, "run_name", f"local_run_name_{uuid.uuid4()}", spark)
    environment   = _get_param(5, "ENV", "dev", spark)
    task_id       = _get_param(6, "task_id", f"local_task_id_{uuid.uuid4()}", spark)


    # Step tracking
    key = f"{pipeline_id}_{run_id}"
    step_index = _STEP_COUNTER.get(key, 0) + 1
    _STEP_COUNTER[key] = step_index

    # Retry attempts
    if attempt_number is None:
        attempt_number = int(_get_widget("task_attempt_number", 1))

    # Cluster / executor info
    worker_node = _get_widget("worker_node", None)
    executor_id = _get_widget("executor_id", None)
    
    # Simple schema - only essential fields
    # schema = StructType([
    #     StructField("pipeline_id", StringType(), True),
    #     StructField("pipeline_name", StringType(), True),
    #     StructField("environment", StringType(), True),
    #     StructField("run_id", StringType(), True),
    #     StructField("run_name", StringType(), True),
    #     StructField("task_id", StringType(), True),
    #     StructField("operation", StringType(), True),
    #     StructField("status", StringType(), True),
    #     StructField("rows", LongType(), True),
    #     StructField("execution_time_ms", LongType(), True),
    #     StructField("source_path", StringType(), True),
    #     StructField("target_path", StringType(), True),
    #     StructField("error_type", StringType(), True),
    #     StructField("error_message", StringType(), True),
    #     StructField("timestamp", TimestampType(), True),
    #     StructField("log_date", StringType(), True),  # Partition key
    # ])

    task_logger_schema = StructType([
    # Step / task identifiers
    StructField("pipeline_id", StringType(), True),
    StructField("pipeline_name", StringType(), True),
    StructField("environment", StringType(), True),  # dev/test/prod
    StructField("run_id", StringType(), True),
    StructField("run_name", StringType(), True),
    StructField("task_id", StringType(), True),
    StructField("step_index", IntegerType(), True),  # step order
    StructField("step_type", StringType(), True),    # read/transform/write etc.
    StructField("parent_task_id", StringType(), True),  # for nested tasks
    StructField("attempt_number", IntegerType(), True), # retry attempt

    # Step execution metrics
    StructField("status", StringType(), True),         # SUCCESS / FAILED
    StructField("rows_processed", LongType(), True),  
    StructField("execution_time_ms", LongType(), True),

    # I/O and source/target tracking
    StructField("source_path", StringType(), True),
    StructField("target_path", StringType(), True),

    # Error tracking
    StructField("error_type", StringType(), True),
    StructField("error_message", StringType(), True),

    # Cluster / executor info (optional but useful)
    StructField("worker_node", StringType(), True),
    StructField("executor_id", StringType(), True),

    # Timestamp and partitioning
    StructField("timestamp", TimestampType(), True),
    StructField("log_date", StringType(), True),  # partition key

    # Optional metadata
    StructField("tags", MapType(StringType(), StringType()), True),
    StructField("etl_metrics", MapType(StringType(), StringType()), True)
])
    
    # Build log entry
    log_entry = Row(
        pipeline_id=pipeline_id,
        pipeline_name=pipeline_name,
        environment=environment,
        run_id=run_id,
        run_name=run_name,
        task_id=task_id,
        step_index=step_index,
        step_type=operation,
        parent_task_id=parent_task_id,
        attempt_number=attempt_number,
        status=status,
        rows_processed=rows,
        execution_time_ms=execution_time_ms,
        source_path=source_path,
        target_path=target_path,
        error_type=error_type,
        error_message=error_message,
        worker_node=worker_node,
        executor_id=executor_id,
        timestamp=None,
        log_date=None,
        tags=tags,
        etl_metrics=etl_metrics
    )
    
    # Create DataFrame
    log_df = (spark.createDataFrame([log_entry], schema=task_logger_schema)
             .withColumn("timestamp", current_timestamp())
             .withColumn("log_date", date_format("timestamp", "yyyy-MM-dd")))
    
    # Write with simple partitioning - just environment and date
    # Write to Delta
    partition_cols = ["environment", "log_date"]
    write_task_log(log_df, environment=environment, partition_cols=partition_cols, file_format=file_format)


# Context manager for automatic logging 
class TaskLogger:
    def __init__(self, operation,log_running=False,**kwargs):
        self.operation = operation
        self.log_running = log_running
        self.kwargs = kwargs
        self.start_time = None

        # Set default context if missing
        # self.kwargs.setdefault("pipeline_id", _get_widget("pipeline_id", "local_run"))
        # self.kwargs.setdefault("pipeline_name", _get_widget("pipeline_name", "unknown_pipeline"))
        # self.kwargs.setdefault("run_id", _get_widget("run_id", f"local_run_{int(time.time())}"))
        # self.kwargs.setdefault("run_name", _get_widget("run_name", f"local_run_name_{uuid.uuid4()}"))
        # self.kwargs.setdefault("environment", _get_widget("ENV", "dev"))

         # Generate a unique task_id if missing
        self.task_id = self.kwargs.get("task_id") or f"local_task_id_{str(uuid.uuid4())}"
        self.kwargs["task_id"] = self.task_id

        
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
