# Imports
import time, uuid
import os
import sys
from pyspark.sql.types import DoubleType, StructType, StructField, IntegerType, StringType
from pyspark.sql import Row, SparkSession
from delta import configure_spark_with_delta_pip
spark = SparkSession.builder.getOrCreate()

# -----------------------------
# Add src to path
# -----------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.abspath(os.path.join(current_dir, '..','..', '..', 'src'))
sys.path.insert(0, src_path)

# -----------------------------
# Custom imports
# -----------------------------
# from unikargo_utils import add_pipeline_metadata
from logging_utils import TaskLogger
from config import get_table_config, get_log_adls_path
from io_utils import _get_widget


import flights, airports
# -----------------------------
# Pipeline / Run metadata
# -----------------------------
# environment = _get_widget("ENV", "dev")
# pipeline_id = _get_widget("pipeline_id", f"local_pipeline_{int(time.time())}")
# # run_id = _get_widget("run_id", f"local_run_{int(time.time())}")
# run_id = sys.argv[2] if len(sys.argv) > 2 else "local_run"
pipeline_id  = sys.argv[1] if len(sys.argv) > 1 else f"local_pipeline_{int(time.time())}"   # job.id
run_id       = sys.argv[2] if len(sys.argv) > 2 else f"local_run_{int(time.time())}"        # job.run_id
task_id      = sys.argv[3] if len(sys.argv) > 3 else f"local_task_{uuid.uuid4()}"           # task.run_id
start_time   = sys.argv[4] if len(sys.argv) > 4 else f"{time.time()}"                        # job.start_time.iso_datetime
run_name     = sys.argv[5] if len(sys.argv) > 5 else f"local_run_name_{uuid.uuid4()}"       # job.run_name
environment  = sys.argv[6] if len(sys.argv) > 6 else "dev"                                   # ENV


# -----------------------------
# Table configs
# -----------------------------
flights_cfg = get_table_config(entity="flights", layer="bronze", environment=environment)
airports_cfg = get_table_config(entity="airports", layer="bronze", environment=environment)
airlines_cfg = get_table_config(entity="airlines", layer="bronze", environment=environment)

# -----------------------------
# Schemas
# -----------------------------
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

airports_schema = StructType([
    StructField("iata_code", StringType(), True),
    StructField("airline", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])

# airline schema
airlines_schema = StructType([
    StructField("iata_code", StringType(), True),
    StructField("airline", StringType(), True)
])

# -----------------------------
# Task registry (all tasks for all entities)
# -----------------------------
task_registry = [
    # Flights
    {
        "entity": "flights",
        "operation": "read",
        "func": lambda df=None, **kwargs: flights.read_flights(spark, flights_schema, flights_cfg)
    },
    {
        "entity": "flights",
        "operation": "transform",
        "func": lambda df, **kwargs: flights.transform_flights(df, **kwargs)
    },
    {
        "entity": "flights",
        "operation": "write",
        "func": lambda df, **kwargs: flights.write_flights_bronze(df, flights_cfg)
    },
    # Airports
    {
        "entity": "airports",
        "operation": "read",
        "func": lambda df=None, **kwargs: airports.read_airports(spark, airports_schema, airports_cfg)
    },
    {
        "entity": "airports",
        "operation": "transform",
        "func": lambda df, **kwargs: airports.transform_airports(df, **kwargs)
    },
    {
        "entity": "airports",
        "operation": "write",
        "func": lambda df, **kwargs: airports.write_airports_bronze(df, airports_cfg)
    },
        # Airlines
    {
        "entity": "airlines",
        "operation": "read",
        "func": lambda df=None, **kwargs: airports.read_airports(spark, airlines_schema, airports_cfg)
    },
    {
        "entity": "airlines",
        "operation": "transform",
        "func": lambda df, **kwargs: airports.transform_airports(df, **kwargs)
    },
    {
        "entity": "airlines",
        "operation": "write",
        "func": lambda df, **kwargs: airports.write_airports_bronze(df, airlines_cfg)
    },
]

# -----------------------------
# Orchestrator function
# -----------------------------
def run_tasks(task_registry, entity_name=None):
    """
    Execute tasks in order. Optionally filter by entity_name.
    Returns a dict of final DataFrames per entity.
    """
    final_dfs = {}
    last_df_per_entity = {}

    for task in task_registry:
        entity = task["entity"]
        if entity_name and entity != entity_name:
            continue

        op = f"{entity}_{task['operation']}"
        func = task["func"]

        try:
            with TaskLogger(
                operation=op,
                pipeline_id=pipeline_id,
                run_id=run_id,
                environment=environment,
                log_running=False,
            ) as logger:

                input_df = last_df_per_entity.get(entity)

                # Pass input_df and logger metadata explicitly
                if task["operation"] == "read":
                    df = func()
                else:
                    df = func(
                        input_df,
                        task_id=logger.task_id,
                        pipeline_id=logger.kwargs.get("pipeline_id"),
                        run_id=logger.kwargs.get("run_id"),
                        environment=environment  # <-- fix here
                    )

                last_df_per_entity[entity] = df

                if df is not None:
                    logger.set_metrics(rows=df.count())

        except Exception as e:
            print(f"Task {op} failed: {e}")
            continue  # <-- allow other entities to proceed

        final_dfs[entity] = last_df_per_entity.get(entity)


    return final_dfs


# -----------------------------
# Examples of usage
# -----------------------------
# Run only flights
flights_df = run_tasks(task_registry, entity_name="flights")["flights"]

# Run only airports
airports_df = run_tasks(task_registry, entity_name="airports")["airports"]

# Run full pipeline
all_final_dfs = run_tasks(task_registry)
flights_df = all_final_dfs["flights"]
airports_df = all_final_dfs["airports"]

# -----------------------------
# Optional: read task logs
# -----------------------------
log_path = get_log_adls_path("task", environment=environment)
logs_df = spark.read.format("delta").load(log_path)
logs_df.orderBy("timestamp", ascending=False).show(20, truncate=False)