# Imports
import time
import os
import sys
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import Row, SparkSession

spark = SparkSession.builder.getOrCreate()
# -----------------------------
# Add src to path
# -----------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.abspath(os.path.join(current_dir, '..', 'src'))
sys.path.insert(0, src_path)

from unikargo_utils import add_pipeline_metadata
from logging_utils import TaskLogger
from config import get_table_config, get_log_adls_path
from io_utils import _get_widget


import flights, airports
# -----------------------------
# Pipeline / Run metadata
# -----------------------------
environment = _get_widget("ENV", "dev")
pipeline_id = _get_widget("pipeline_id", f"local_pipeline_{int(time.time())}")
run_id = _get_widget("run_id", f"local_run_{int(time.time())}")

# -----------------------------
# Table configs
# -----------------------------
flights_cfg = get_table_config(entity="flights", layer="bronze", environment=environment)
airports_cfg = get_table_config(entity="airports", layer="bronze", environment=environment)

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
    StructField("departure_delay", IntegerType(), True),
    StructField("arrival_delay", IntegerType(), True),
])

airports_schema = StructType([
    StructField("airport_code", StringType(), True),
    StructField("airport_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
])

# -----------------------------
# Task registry
# -----------------------------
task_registry = [
    {
        "operation": "read_flights_raw",
        "func": lambda df=None: flights.read_flights(spark, flights_schema, flights_cfg)
    },
    {
        "operation": "transform_flights",
        "func": lambda df: flights.transform_flights(df)
    },
    {
        "operation": "write_flights_bronze",
        "func": lambda df: flights.write_flights_bronze(df, flights_cfg)
    },
    {
        "operation": "read_airports_raw",
        "func": lambda df=None: airports.read_airports(spark, airports_schema, airports_cfg)
    },
    {
        "operation": "transform_airports",
        "func": lambda df: airports.transform_airports(df)
    },
    {
        "operation": "write_airports_bronze",
        "func": lambda df: airports.write_airports_bronze(df, airports_cfg)
    },
]

# -----------------------------
# Orchestrator loop
# -----------------------------
# dfs = {}  # store last dataframe for each entity
# for task in task_registry:
#     op = task["operation"]
#     try:
#         with TaskLogger(
#             operation=op,
#             pipeline_id=pipeline_id,
#             run_id=run_id,
#             environment=environment,
#             log_running=False
#         ) as logger:
#             if "read" in op:
#                 df = task["func"]()
#                 dfs[op] = df
#             else:
#                 # Use last df processed for entity
#                 entity_key = op.split("_")[1]
#                 df = task["func"](dfs.get(f"read_{entity_key}_raw"))
#                 dfs[f"{op}"] = df

#             # Log rows processed if dataframe exists
#             if df is not None:
#                 logger.set_metrics(rows=df.count())

#     except Exception as e:
#         print(f"Task {op} failed: {e}")
#         break

# def run_pipeline_for_entity(entity_name, task_registry, pipeline_id, run_id, environment):
#     dfs = {}
#     # Filter tasks for this entity
#     entity_tasks = [t for t in task_registry if entity_name in t["operation"]]

#     for task in entity_tasks:
#         op = task["operation"]
#         try:
#             with TaskLogger(
#                 operation=op,
#                 pipeline_id=pipeline_id,
#                 run_id=run_id,
#                 environment=environment,
#                 log_running=False
#             ) as logger:

#                 if "read" in op:
#                     df = task["func"]()
#                     dfs[op] = df
#                 else:
#                     # Use last dataframe for entity
#                     last_read_key = f"read_{entity_name}_raw"
#                     df = task["func"](dfs.get(last_read_key))
#                     dfs[op] = df

#                 # Log rows processed
#                 if df is not None:
#                     logger.set_metrics(rows=df.count())

#         except Exception as e:
#             print(f"Task {op} failed: {e}")
#             break
#     return dfs  # optionally return the last dataframes

def run_pipeline_for_entity(entity_name, task_registry, pipeline_id, run_id, environment):
    """
    Execute all tasks for a specific entity in order:
    read -> transform -> write
    
    Returns the final dataframe produced for the entity.
    """
    dfs = {}  # store last dataframe for each task
    
    for op, task_func in task_registry.items():
        # Only pick tasks for this entity
        if not op.startswith(entity_name):
            continue

        try:
            with TaskLogger(
                operation=op,
                pipeline_id=pipeline_id,
                run_id=run_id,
                environment=environment,
                log_running=False
            ) as logger:

                if "read" in op:
                    df = task_func()
                else:
                    # Use the last dataframe for this entity
                    df = task_func(dfs.get(f"{entity_name}_read_raw"))

                dfs[op] = df

                if df is not None:
                    logger.set_metrics(rows=df.count())

        except Exception as e:
            print(f"Task {op} failed: {e}")
            break

    # Return only the final dataframe for convenience
    last_task_key = [k for k in dfs if k.startswith(entity_name)][-1]
    return dfs[last_task_key]


# # Run only flights
# dfs_flights = run_pipeline_for_entity("flights", task_registry, pipeline_id, run_id, environment)

# # Run only airports
# dfs_airports = run_pipeline_for_entity("airports", task_registry, pipeline_id, run_id, environment)

# Run flights tasks only
final_flights_df = run_pipeline_for_entity(
    entity_name="flights",
    task_registry=task_registry,
    pipeline_id=pipeline_id,
    run_id=run_id,
    environment=environment
)

# Run airports tasks only
final_airports_df = run_pipeline_for_entity(
    entity_name="airports",
    task_registry=task_registry,
    pipeline_id=pipeline_id,
    run_id=run_id,
    environment=environment
)

# Run all entities
for entity in ["flights", "airports"]:
    run_pipeline_for_entity(entity, task_registry, pipeline_id, run_id, environment)


# Run flights tasks only
final_flights_df = run_pipeline_for_entity(
    entity_name="flights",
    task_registry=task_registry,
    pipeline_id=pipeline_id,
    run_id=run_id,
    environment=environment
)

# Run airports tasks only
final_airports_df = run_pipeline_for_entity(
    entity_name="airports",
    task_registry=task_registry,
    pipeline_id=pipeline_id,
    run_id=run_id,
    environment=environment
)

def run_full_pipeline(task_registry, pipeline_id, run_id, environment):
    """
    Run the entire pipeline for all entities defined in task_registry.
    
    Returns a dict mapping entity names to their final DataFrame.
    """
    final_dfs = {}
    
    # Extract unique entity names from task keys
    entities = set(op.split("_")[0] for op in task_registry.keys())
    
    for entity in entities:
        print(f"Running pipeline for entity: {entity}")
        final_df = run_pipeline_for_entity(
            entity_name=entity,
            task_registry=task_registry,
            pipeline_id=pipeline_id,
            run_id=run_id,
            environment=environment
        )
        final_dfs[entity] = final_df
    
    return final_dfs

# Run the full pipeline for all entities
all_final_dfs = run_full_pipeline(
    task_registry=task_registry,
    pipeline_id=pipeline_id,
    run_id=run_id,
    environment=environment
)

# -----------------------------
# Optional: read task logs
# -----------------------------
log_path = get_log_adls_path("task", environment=environment)
logs_df = spark.read.format("delta").load(log_path)
logs_df.orderBy("timestamp", ascending=False).show(20, truncate=False)
