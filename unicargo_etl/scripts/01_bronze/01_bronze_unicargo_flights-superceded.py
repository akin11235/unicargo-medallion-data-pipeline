import os
import sys
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..', 'src')) # Go up 3 levels and append 'src'
sys.path.append(project_root) # Add src to sys.path

from logging_utils import TaskLogger
from unikargo_utils import add_pipeline_metadata
from config import get_log_adls_path, get_table_config
from io_utils import _get_widget


rows_processed = 0
log_type =  'task'
environment = _get_widget("ENV", "dev")
run_id = _get_widget("run_id")
# run_name = _get_widget("run_name")

entity="airports"
layer="bronze"
flights_cfg = get_table_config(entity="flights", layer="bronze", environment=environment)
print(flights_cfg)


flights_schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("airline", StringType(), True),
    StructField("flight_number", IntegerType(), True),
    StructField("tail_number", StringType(), True),
    StructField("origin_airport", StringType(), True),
    StructField("destination_airport", StringType(), True),
    StructField("scheduled_departure", IntegerType(), True),
    StructField("departure_time", IntegerType(), True),
    StructField("departure_delay", IntegerType(), True),
    StructField("taxi_out", IntegerType(), True),
    StructField("wheels_off", IntegerType(), True),
    StructField("scheduled_time", IntegerType(), True),
    StructField("elapsed_time", IntegerType(), True),
    StructField("air_time", IntegerType(), True),
    StructField("distance", IntegerType(), True),
    StructField("wheels_on", IntegerType(), True),
    StructField("taxi_in", IntegerType(), True),
    StructField("scheduled_arrival", IntegerType(), True),
    StructField("arrival_time", IntegerType(), True),
    StructField("arrival_delay", IntegerType(), True),
    StructField("diverted", IntegerType(), True),
    StructField("cancelled", IntegerType(), True),
    StructField("cancellation_reason", StringType(), True),
    StructField("air_system_delay", IntegerType(), True),
    StructField("security_delay", IntegerType(), True),
    StructField("airline_delay", IntegerType(), True),
    StructField("late_aircraft_delay", IntegerType(), True),
    StructField("weather_delay", IntegerType(), True),
])

# -----------------------------
# Read the flights CSV
# -----------------------------
flights_csv_path = flights_cfg.raw_path
operation = "tsk_flights_read_raw"

with TaskLogger(
    operation=operation,
    # pipeline_name=pipeline_name,
    source_path=flights_csv_path,
    log_running=False  # keep this False unless you explicitly want a "RUNNING" entry
) as logger:

    flights_df = (spark.read
        .schema(flights_schema)
        .option("header", "true") 
        .csv(flights_csv_path) 
)
    
    rows_processed = flights_df.count()
        
    # Update metrics before completion
    logger.set_metrics(rows=rows_processed)


# -----------------------------
# --- Task 2: Add metadata to the dataframe 
# -----------------------------
operation="tsk_flights_add_metadata"

with TaskLogger(
    operation=operation,
    # pipeline_name=pipeline_name,
    log_running=False 
) as logger:

    # flights_df_df = add_pipeline_metadata(flights_df, pipeline_id, run_id, task_id)

    flights_df = add_pipeline_metadata(
    flights_df,
    pipeline_id=logger.kwargs.get("pipeline_id"),
    run_id=logger.kwargs.get("run_id"),
    task_id=logger.kwargs.get("task_id")
)

    # Count rows after transformation
    rows_processed = flights_df.count()

    # Update metrics before completion
    logger.set_metrics(rows=rows_processed)



    # -----------------------------
# Write to bronze
# -----------------------------
target_path = flights_cfg.full_name
print("target_path: ", target_path)
operation = "tsk_flights_persist_bronze"


with TaskLogger(
    operation=operation,
    # pipeline_name=pipeline_name,
    target_path=target_path,
    log_running=False
) as logger:

    # Count rows first
    rows_processed = flights_df.count()

    flights_df.write.\
    mode("overwrite").\
    option("overwriteSchema", "true").\
    saveAsTable(target_path)

    # Update metrics before completion
    logger.set_metrics(rows=rows_processed)


flights_csv_path = flights_cfg.raw_path
operation = "tsk_flights_read_raw_error"

try:
    with TaskLogger(
        operation=operation,
        # pipeline_name=pipeline_name,
        source_path=flights_csv_path,
        log_running=False
    ) as logger:

        # Simulate reading flights data
        flights_df = (spark.read
            .schema(flights_schema)
            .option("header", "true")
            .csv(flights_csv_path)
        )

        # Artificially trigger an error
        raise ValueError("Simulated failure for testing TaskLogger")

        rows_processed = flights_df.count()
        logger.set_metrics(rows=rows_processed)

except Exception as e:
    print(f"Caught error: {e}")


# --------Uncomment to debug (Read Delta logs and show latest logs)-----------------

log_path = get_log_adls_path(log_type, environment=environment) # Path to save logging for tasks
logs_df = spark.read.format("delta").load(log_path)
logs_df.orderBy("timestamp", ascending=False).show(20, truncate=False)