#  Logging utility

import time, uuid
from datetime import datetime
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.functions import current_timestamp, date_format

from io_utils.write_to_table_utils import write_pipeline_log
from io_utils.widget_utils import _get_widget


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


    # --- Create Row ---
    log_entry = Row(
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


    # --- Create Spark DataFrame with timestamp ---
    log_df = (
        spark.createDataFrame([log_entry], schema=log_schema)
             .withColumn("timestamp", current_timestamp())
             .withColumn("log_date", date_format("timestamp", "yyyy-MM-dd"))
    )

    # --- Partition by pipeline_id + name + environment ---
    partition_cols = ["pipeline_id", "pipeline_name", "environment"]

    # --- Write using _safe_write ---
    # _safe_write(log_df, LOG_PATH_PIPELINE, partition_cols, file_format=file_format)
    write_pipeline_log(log_df, environment="dev", partition_cols=partition_cols, file_format=file_format)


# -----------------------
# Context manager for pipeline logging
# -----------------------
class PipelineLogger:
    def __init__(self, pipeline_name, catalog=None, pipeline_id=None, log_running=True, **kwargs):
        self.pipeline_name = pipeline_name
        self.catalog = catalog
        self.pipeline_id = pipeline_id
        self.log_running = log_running
        self.kwargs = kwargs
        self.run_id = None
        self.start_time = None

    def __enter__(self):
        self.start_time = datetime.now()
        self.run_id = self.kwargs.get("run_id") or f"local_run_{int(time.time())}"

        # Log RUNNING state at pipeline start (optional)
        if self.log_running:
            log_pipeline_event(
                pipeline_name=self.pipeline_name,
                catalog=self.catalog,
                pipeline_id=self.pipeline_id,
                run_id=self.run_id,
                status="RUNNING",
                start_time=self.start_time,
                **self.kwargs
            )

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        status = "SUCCESS" if exc_type is None else "FAILED"

        log_pipeline_event(
            pipeline_name=self.pipeline_name,
            catalog=self.catalog,
            pipeline_id=self.pipeline_id,
            run_id=self.run_id,
            status=status,
            start_time=self.start_time,
            end_time=end_time,
            message=str(exc_val)[:200] if exc_val else "",
            **self.kwargs
        )

        # Do not suppress exceptions
        return False

    def set_metrics(self, **metrics):
        """Update dynamic metrics (e.g., row counts) during pipeline execution."""
        self.kwargs.update(metrics)


