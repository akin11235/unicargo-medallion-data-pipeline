# We’ll use two tables:

# pipeline_logs → 1 record per pipeline run.

# task_logs → 1 record per task/stage in the pipeline.

from datetime import datetime
import uuid
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from pyspark.sql import Row, SparkSession

spark = SparkSession.builder.getOrCreate()


# -----------------------
# Log Schemas
# -----------------------
pipeline_log_schema = StructType([
    StructField("pipeline_id", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("rows_processed", LongType(), True),
    StructField("error_message", StringType(), True)
])

task_log_schema = StructType([
    StructField("pipeline_id", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("task_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("rows", LongType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("error_message", StringType(), True)
])

# -----------------------
# Helpers
# -----------------------
def new_run_id():
    return str(uuid.uuid4())

# -----------------------
# Log Functions
# -----------------------
def log_task(pipeline_id, run_id, task_id, status, rows=None, error_message=None):
    log_entry = [Row(
        pipeline_id=pipeline_id,
        run_id=run_id,
        task_id=task_id,
        status=status,
        rows=rows,
        timestamp=datetime.utcnow(),
        error_message=error_message
    )]
    spark.createDataFrame(log_entry, task_log_schema) \
         .write.mode("append") \
         .saveAsTable("logs.task_logs")

def log_pipeline(pipeline_id, run_id, status, start_time, end_time, rows_processed=None, error_message=None):
    log_entry = [Row(
        pipeline_id=pipeline_id,
        run_id=run_id,
        status=status,
        start_time=start_time,
        end_time=end_time,
        rows_processed=rows_processed,
        error_message=error_message
    )]
    spark.createDataFrame(log_entry, pipeline_log_schema) \
         .write.mode("append") \
         .saveAsTable("logs.pipeline_logs")



# Step 3: Apply in Your Pipeline
# from datetime import datetime

# pipeline_id = "airlines_ingestion"
# run_id = str(uuid.uuid4())
# start_time = datetime.utcnow()

# try:
#     # --- Task 1: Read
#     df = (spark.read
#             .schema(airlines_schema)
#             .option("header", "true")
#             .csv("abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/raw/volumes/airlines.csv"))

#     log_task(pipeline_id, run_id, "read_raw", "SUCCESS", rows=df.count())

#     # --- Task 2: Transform
#     df = df.withColumn("metadata",
#                        create_map(
#                            lit("pipeline_id"), lit(pipeline_id),
#                            lit("run_id"), lit(run_id),
#                            lit("task_id"), lit("transform"),
#                            lit("processed_timestamp"), lit(datetime.utcnow().isoformat())
#                        ))
#     log_task(pipeline_id, run_id, "transform", "SUCCESS", rows=df.count())

#     # --- Task 3: Write
#     df.write.mode("overwrite").option("overwriteSchema", "true") \
#        .saveAsTable("bronze.unikargo_airlines_bronze")

#     log_task(pipeline_id, run_id, "write_bronze", "SUCCESS", rows=df.count())

#     # --- Final pipeline log
#     log_pipeline(pipeline_id, run_id, "SUCCESS", start_time, datetime.utcnow(), rows_processed=df.count())

# except Exception as e:
#     error_msg = str(e)
#     log_task(pipeline_id, run_id, "FAILED_TASK", "FAILED", error_message=error_msg)
#     log_pipeline(pipeline_id, run_id, "FAILED", start_time, datetime.utcnow(), error_message=error_msg)
#     raise
