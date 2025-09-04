#  Logging utility

# Do you want me to also add a schema enforcement layer (so logs don’t drift if someone adds new fields accidentally), or do you prefer the lightweight Row-based approach you’re using now?

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import current_timestamp
import time, uuid
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.utils import AnalysisException


spark = SparkSession.builder.getOrCreate()

# Configurable log storage path
LOG_PATH = "abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/logs/pipeline_completion"
MAX_RETRIES = 3
RETRY_DELAY = 5

LOG_PATH_PIPELINE = "abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/logs/pipeline_logs"
LOG_PATH_TASK = "abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/logs/task_logs"


# -----------------------
# Helpers
# -----------------------
def new_run_id():
    """Generate a unique run_id for a pipeline execution."""
    return str(uuid.uuid4())


"""
1. Delta Format
    Delta Lake is essentially an enhanced Parquet with added features.
    Advantages:

    ACID transactions: ensures your writes are atomic, consistent, isolated, and durable. This is crucial for logging or ETL pipelines where partial writes can corrupt data.
    Schema enforcement and evolution: Delta can reject invalid data or allow safe schema changes.
    Time travel: you can query the state of the table at a previous version.
    Upserts / merges: supports MERGE INTO for updating existing rows.
    Example for logs:
    If your logging fails halfway and retries, Delta guarantees you won't get partially written or duplicate rows.
vs

2 Parquet Format
    Parquet is just a columnar storage format.
    Advantages:
        Very fast for reading/writing columnar data.
        Smaller file sizes due to compression.
        Limitations for logs:
        No ACID guarantees → failed writes may leave corrupted or duplicate files.
        No time travel or versioning.
        No built-in support for merge/upsert → harder to update existing rows safely.
"""

def _safe_write(log_df, path, partition_cols, file_format="delta"):
    """
    Retry-safe write to Delta or Parquet, auto-creating the table if it doesn't exist.

    Parameters:
        log_df (DataFrame): Spark DataFrame to write
        path (str): target path
        partition_cols (list): columns to partition by
        file_format (str): "delta" (default) or "parquet"
    """
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            writer = log_df.write.mode("append").partitionBy(*partition_cols)

            if file_format.lower() == "delta":
                # Try writing; if table doesn't exist, create it
                try:
                    writer.format("delta").save(path)
                except AnalysisException as e:
                    # If path doesn't exist, create Delta table
                    if "Path does not exist" in str(e) or "Table or view not found" in str(e):
                        writer.format("delta").option("overwriteSchema", "true").save(path)
                    else:
                        raise

            elif file_format.lower() == "parquet":
                writer.format("parquet").save(path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")

            return  # success

        except Exception as e:
            attempt += 1
            print(f"Logging attempt {attempt} failed: {e}")
            time.sleep(RETRY_DELAY)

    raise RuntimeError(f"Failed to write logs after {MAX_RETRIES} attempts.")

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
    import time, uuid
    from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
    from pyspark.sql.functions import current_timestamp

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

    # --- Create DataFrame ---
    log_df = spark.createDataFrame([(
        pipeline_id,
        pipeline_name,
        environment,
        run_id,
        status,
        start_time,
        end_time,
        duration,
        rows_processed,
        message,
        None
    )], schema=log_schema).withColumn("timestamp", current_timestamp())

    # --- Partition by pipeline_id + name + environment ---
    partition_cols = ["pipeline_id", "pipeline_name", "environment"]

    # --- Write using _safe_write ---
    _safe_write(log_df, LOG_PATH_PIPELINE, partition_cols, file_format=file_format)
# ----------------------
# Task-level logging
# ----------------------
# 2. Task-level logging (per stage or operation)
# Each notebook stage (Extract, Transform, Load) logs its own status.
# You might log after:
    # Read from ADLS (rows read)
    # Transform (rows after filtering/joins)
    # Write (rows written, target table name)

# This helps when debugging issues — you’ll know exactly which step failed.

# LOG_PATH_TASK = "abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/logs/task_logs"

def log_task_event(
    status, 
    rows=None, 
    message="", 
    pipeline_name=None, 
    pipeline_id=None, 
    file_format="delta"
):
    """
    Write structured task-level logs to Delta (partitioned),
    automatically using ADF widgets (or defaults if run manually).
    """

    # --- Get pipeline/run/task identifiers ---
    pipeline_id   = pipeline_id or _get_widget("pipeline_id", str(uuid.uuid4()))
    pipeline_name = pipeline_name or _get_widget("pipeline_name", "pipeline_name")
    run_id = _get_widget("run_id", f"local_run_{int(time.time())}")
    task_id = _get_widget("task_id", "local_test")
    environment  = _get_widget("catalog", "unikargo_dev")


    # # Create a Row for logging
    # log_row = Row(
    #     pipeline_name=pipeline_name,
    #     catalog=catalog,
    #     run_id=run_id,
    #     task_id=task_id,
    #     status=status,
    #     rows=rows,
    #     message=message
    # )

    # # Create a Spark DataFrame with automatic timestamp column
    # log_df = spark.createDataFrame([log_row]).withColumn("timestamp", current_timestamp())

    # --- Define schema ---
    log_schema = StructType([
        StructField("pipeline_id", StringType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("environment", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField("task_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("rows", LongType(), True),
        StructField("message", StringType(), True),
        StructField("timestamp", TimestampType(), True),
    ])


    # --- Create DataFrame for logging ---
    log_df = spark.createDataFrame([(
        pipeline_id,
        pipeline_name,
        environment,
        run_id,
        task_id,
        status,
        rows,
        message,
        None
    )], schema=log_schema).withColumn("timestamp", current_timestamp())

    # Partition by columns including pipeline_id for uniqueness
    partition_cols = ["pipeline_id", "pipeline_name", "environment", "task_id"]

    # append to Delta table
    _safe_write(log_df, LOG_PATH_TASK, partition_cols, file_format=file_format)

    # Write logs as Parquet
    # _safe_write(log_df, LOG_PATH_TASK, ["pipeline_name", "environment", "task_id"], file_format="parquet")



# def log_task_event(pipeline_name, environment, run_id, task_id, status, rows=None, message=""):
#     """
#     Write structured task-level logs to Delta (partitioned),
#     automatically using ADF widgets (or defaults if run manually).
#     """
#     # Get widget values with safe defaults
#     pipeline_name = _get_widget("pipeline_id", "manual_pipeline")
#     run_id = _get_widget("run_id", f"manual_run_{int(time.time())}")
#     task_id = _get_widget("task_id", "manual_task")
#     environment = _get_widget("catalog", "unikargo_dev")

#     log_row = Row(



#         pipeline_name=pipeline_name,
#         environment=environment,
#         run_id=run_id,
#         task_id=task_id,
#         status=status,
#         rows=rows,
#         timestamp=datetime.timezone.utcnow(),
#         message=message
#     )

#     log_df = spark.createDataFrame([log_row])
#     _safe_write(log_df, f"{LOG_PATH_TASK}/task_logs", ["pipeline_name", "environment", "task_id"])

# -----------------------
# Task Logs
# -----------------------
# def log_task_event(pipeline_name,
#                    environment,
#                    run_id,
#                    task_id,
#                    status,
#                    rows=None,
#                    message=""):
#     """
#     Write structured task-level logs to Delta (partitioned).
#     """
#     log_row = Row(
#         pipeline_name=pipeline_name,
#         environment=environment,
#         run_id=run_id,
#         task_id=task_id,
#         status=status,
#         rows=rows,
#         timestamp=datetime.utcnow(),
#         message=message
#     )

#     log_df = spark.createDataFrame([log_row])
#     _safe_write(log_df, f"{LOG_PATH}/task_logs", ["pipeline_name", "environment", "task_id"])  






# def log_pipeline_event(pipeline_name,
#                        environment,
#                         run_id,
#                         status,
#                         start_time,
#                         end_time,
#                         rows_processed=0,
#                         message=""):
#     """
#     Write structured pipeline event logs to ADLS as Delta.
#     """
#     duration = (end_time - start_time).total_seconds()

#     log_row = Row(
#         pipeline_name=pipeline_name,
#         environment=environment,
#         run_id=run_id,
#         status=status,
#         start_time=start_time,
#         end_time=end_time,
#         duration=duration,
#         rows_processed=rows_processed,
#         message=message
#     )

#     log_df = spark.createDataFrame([log_row])

#     attempt = 0
#     while attempt < MAX_RETRIES:
#         try:
#             log_df.write.format("delta") \
#                 .mode("append") \
#                 .partitionBy("pipeline_name", "environment") \
#                 .save(LOG_PATH)
#             break
#         except Exception as e:
#             attempt += 1
#             print(f"Logging attempt {attempt} failed: {e}")
#             time.sleep(RETRY_DELAY)