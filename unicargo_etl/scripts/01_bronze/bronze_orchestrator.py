import json, os, sys, time, uuid
from pathlib import Path
from pyspark.sql import SparkSession
import argparse


# Add src folder to sys.path
# current_dir = os.path.dirname(os.path.abspath(__file__))
try:
    # Normal Python script
    current_dir = Path(__file__).resolve().parent
except NameError:
    # Notebook / Databricks Connect
    current_dir = Path.cwd()

src_path = os.path.abspath(os.path.join(current_dir, '..','..', '..', 'src'))

if src_path not in sys.path:
    sys.path.insert(0, src_path)  

from logging_utils import TaskLogger
from config import get_table_config, get_log_adls_path
from unikargo_utils import add_pipeline_metadata
# SparkSession (using :contentReference[oaicite:0]{index=0} Connect)
# from databricks.connect import DatabricksSession
# spark = DatabricksSession.builder.getOrCreate()
spark = SparkSession.builder.getOrCreate()
# -----------------------------
# Pipeline / Run metadata
# -----------------------------
# pipeline_id  = sys.argv[1] if len(sys.argv) > 1 else f"local_pipeline_{int(time.time())}"   # job.id
# run_id       = sys.argv[2] if len(sys.argv) > 2 else f"local_run_{int(time.time())}"        # job.run_id
# task_id      = sys.argv[3] if len(sys.argv) > 3 else f"local_task_{uuid.uuid4()}"           # task.run_id
# start_time   = sys.argv[4] if len(sys.argv) > 4 else f"{time.time()}"                        # job.start_time.iso_datetime
# run_name     = sys.argv[5] if len(sys.argv) > 5 else f"local_run_name_{uuid.uuid4()}"       # job.run_name
# environment  = sys.argv[1] if len(sys.argv) > 6 else "dev"       

parser = argparse.ArgumentParser()
# Pipeline-level parameters
parser.add_argument("--environment", default="dev")
parser.add_argument("--pipeline_id", default=f"local_pipeline_{int(time.time())}")
parser.add_argument("--pipeline_name", default="local_pipeline")
parser.add_argument("--run_id", default=f"local_run_{int(time.time())}")
parser.add_argument("--start_time", default=f"{time.time()}")
# parser.add_argument("--run_name", default=f"local_run_name_{uuid.uuid4()}")

# Optional extra metadata (safe to keep, just default to empty)
# parser.add_argument("--catalog", default="")
parser.add_argument("--task_id", default=f"local_task_{uuid.uuid4()}")
parser.add_argument("--step_index", default="0")
# parser.add_argument("--operation", default="orchestrate")
parser.add_argument("--parent_task_id", default="")
parser.add_argument("--attempt_number", default="1")
parser.add_argument("--task_run_id", default=f"local_task_{uuid.uuid4()}")
# parser.add_argument("--source", default="")
# parser.add_argument("--target", default="")
parser.add_argument("--worker_node", default="")
parser.add_argument("--executor_id", default="")
parser.add_argument("--tags", default="{}")
parser.add_argument("--etl_metrics", default="{}")

# args = parser.parse_args()
# THIS IS THE KEY LINE - use parse_known_args() not parse_args()
args, unknown_args = parser.parse_known_args()

# 2. Handle positional unknown args
if unknown_args and len(unknown_args) >= 1:
    args.environment = unknown_args[0]

print(f"Parsed args: {args}")
print(f"Unknown args (ignored): {unknown_args}")  # This should show ['dev']

# Convert JSON-like strings to dicts
tags = json.loads(args.tags) if args.tags else {}
etl_metrics = json.loads(args.etl_metrics) if args.etl_metrics else {}


# 4. Override with Databricks context if running on a job
try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    args.pipeline_id = ctx.jobId().get() if ctx.jobId().isDefined() else args.pipeline_id
    args.run_id = ctx.jobRunId().get() if ctx.jobRunId().isDefined() else args.run_id
    args.pipeline_name = ctx.tags().apply("jobName") if ctx.tags().contains("jobName") else args.pipeline_name
except Exception:
    pass  # local run
# -----------------------------
# Table configs
# -----------------------------
# env = "dev"
from schemas.flights_schema import flights_schema
from schemas.airports_schema import airports_schema
from schemas.airlines_schema import airlines_schema

# args.environment = args.environment.replace("--environment=", "")
flights_cfg = get_table_config(entity="flights", layer="bronze", environment=args.environment)
airports_cfg = get_table_config(entity="airports", layer="bronze", environment=args.environment)
airlines_cfg = get_table_config(entity="airlines", layer="bronze", environment=args.environment)

# airlines_csv_path = airlines_cfg.raw_path
# airports_csv_path = airports_cfg.raw_path
# flights_csv_path = flights_cfg.raw_path
# -----------------------------
# Get tasks with explicit ordering
# -----------------------------
# from tasks.flights_tasks import get_flights_tasks
# from tasks.airports_tasks import get_airports_tasks
# from tasks.airlines_tasks import get_airlines_tasks

# task_registry = (
#     get_flights_tasks(spark, flights_schema, flights_cfg)
#     + get_airports_tasks(spark, airports_schema, airports_cfg)
#     + get_airlines_tasks(spark, airlines_schema, airlines_cfg)
# )
from task_factory import create_entity_tasks
from business_logic import transform_airlines_logic, transform_airports_logic, transform_flights_logic

# Standard entities with metadata
task_registry = (
    create_entity_tasks("flights", spark, flights_schema, flights_cfg, transform_flights_logic) +
    create_entity_tasks("airports", spark, airports_schema, airports_cfg, transform_airports_logic) +  
    create_entity_tasks("airlines", spark, airlines_schema, airlines_cfg)  # No transform needed
)


# Skip metadata for specific entity (maybe for testing)
# task_registry = (
#     create_entity_tasks("flights", spark, flights_schema, flights_cfg, transform_flights_logic, add_metadata=False) +
#     create_entity_tasks("airports", spark, airports_schema, airports_cfg, transform_airports_logic) +  
#     create_entity_tasks("airlines", spark, airlines_schema, airlines_cfg)
# )
# tags = {
#     "source_system": "flights_api",
#     "pipeline_version": "v1.0.3",
#     "team": "data_eng"
# }
from pyspark.sql.functions import col
# Generic orchestrator
def run_tasks(task_registry, entity_name=None):
    final_dfs, last_df_per_entity = {}, {}

    for task in task_registry:
        if entity_name and task["entity"] != entity_name: 
            continue

        op = f"{task['entity']}_{task['operation']}"

        try:
            with TaskLogger(
                operation=op,
                source_path=task.get("source_path"),
                target_path=task.get("target_path"),
                pipeline_id=args.pipeline_id,
                pipeline_name=args.pipeline_name,
                run_id=args.run_id,
                task_id=args.task_id,
                tags=tags,
                etl_metrics={}
                ) as logger:
                df_in = last_df_per_entity.get(task["entity"])
                
                # Execute task
                if df_in is not None:
                    df_out = task["func"](df_in)
                else:
                    df_out = task["func"]()

                # Add metadata ONLY after transform step
                if df_out is not None and task["operation"] == "transform":
                    df_out = add_pipeline_metadata(
                        df_out,
                        pipeline_id=args.pipeline_id,
                        run_id=args.run_id,
                        task_id=logger.task_id
                        # task_id=args.task_id
                    )

                last_df_per_entity[task["entity"]] = df_out

                # Compute metrics if df_out exists
                if df_out is not None:
                    # logger.set_metrics(rows=df_out.count())
                    # etl_metrics = {}
                    etl_metrics = {
                    "rows_processed": df_out.count(),
                    "columns_processed": len(df_out.columns),
                    "null_counts": {
                        col_name: df_out.filter(col(col_name).isNull()).count()
                        for col_name in df_out.columns
                        }
                    }
                    # if "weather_delay" in df_out.columns:
                    #     # etl_metrics["null_weather_delay_count"] = df_out.filter(col("weather_delay").isNull()).count()
                    #     etl_metrics["rows_processed"] = df_out.count()
                    #     etl_metrics["columns_processed"] = len(df_out.columns)
                    #     etl_metrics["null_counts"] = {
                    #         col_name: df_out.filter(col(col_name).isNull()).count()
                    #         for col_name in df_out.columns
                    #     }

                    # else:
                    #     etl_metrics = {"rows_processed": 0, "columns_processed": 0, "null_counts": {}}
                    logger.set_metrics(
                        # rows=df_out.count(),
                        rows=etl_metrics["rows_processed"],
                        etl_metrics=etl_metrics
                    )
                    # logger.set_metrics(
                    #     rows=df_out.count(),
                    #     columns=len(df_out.columns),
                    #     execution_time_ms=int((time.time() - start_time) * 1000)
                    # )


        except Exception as e:
            print(f"Task {op} failed: {e}")

        final_dfs[task["entity"]] = last_df_per_entity.get(task["entity"])

    return final_dfs

# Run pipeline
all_results = run_tasks(task_registry)


# --------Uncomment to debug (Read Delta logs and show latest logs)-----------------
# log_type =  'task'
# log_path = get_log_adls_path(log_type, environment=args.environment) # Path to save logging for tasks
# logs_df = spark.read.format("delta").load(log_path)
# logs_df.orderBy("timestamp", ascending=False).show(20, truncate=False)