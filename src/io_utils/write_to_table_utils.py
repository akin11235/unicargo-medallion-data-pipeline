import sys
sys.path.append('..')
from config import get_table_config, get_log_config
from datetime import datetime
import time
from pyspark.sql.utils import AnalysisException


# -------------------------------
# Core save function
# -------------------------------
def save_to_table(
        df, 
        entity: str,
        layer: str,
        environment: str = "dev", 
        table_key: str = None,
        mode: str = "overwrite", 
        overwrite_schema: bool = True
    ):
    """
    Save a Spark DataFrame to the specified layer table using configuration.
    
    Parameters:
    - df: Spark DataFrame to save
    - entity: Entity type ('airlines', 'airports', 'flights')
    - layer: Logical layer ('bronze', 'silver', 'gold')
    - environment: Environment to save to ('dev', 'staging', 'prod')
    - table_key: Required for gold layer tables (e.g. 'daily_performance')
    - mode: Spark write mode ('overwrite', 'append', etc.)
    - overwrite_schema: Whether to overwrite the table schema (default: True)
    """

    config = get_table_config(
        entity=entity,
        layer=layer,
        environment=environment,
        table_key=table_key # Will be None for bronze/silver
    )
    
    writer = df.write.mode(mode)
    
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    
    writer.saveAsTable(config.full_name)
    print(f"Saved to: {config.full_name} (overwriteSchema: {overwrite_schema})")
    return config


# --------------------------
# Convenience wrappers
# --------------------------

def save_to_bronze(df, entity: str, environment: str = "dev", **kwargs):
    """Save DataFrame to Bronze layer"""
    return save_to_table(df, entity=entity, layer="bronze", environment=environment, **kwargs)


def save_to_silver(df, entity: str, environment: str = "dev", **kwargs):
    """Save DataFrame to Silver layer"""
    return save_to_table(df, entity=entity, layer="silver", environment=environment, **kwargs)


def save_to_gold(df, entity: str, table_key: str, environment: str = "dev", **kwargs):
    """Save DataFrame to Gold layer (requires table_key)"""
    return save_to_table(df, entity=entity, layer="gold", environment=environment, table_key=table_key, **kwargs)

# # Usage examples:
# save_to_gold_table(cancellation_rates_airline, "airline_cancellation_rates", "prod")
# save_to_gold_table(daily_summary_df, "daily_flight_summary", "staging")

# # If you don't want to overwrite schema for some reason:
# save_to_gold_table(route_analysis_df, "route_analysis", "dev", overwrite_schema=False)


# Or even simpler, always with overwriteSchema:
# def save_to_gold_table(df, table_key: str, environment: str = "dev", mode: str = "overwrite"):
#     """Save DataFrame to gold layer with schema overwrite enabled"""
#     config = get_table_config(
#         entity="03_gold",
#         layer="03_gold", 
#         environment=environment,
#         table_key=table_key
#     )
    
#     df.write \
#         .mode(mode) \
#         .option("overwriteSchema", "true") \
#         .saveAsTable(config.full_name)
    
#     print(f"Saved to: {config.full_name} (schema overwrite enabled)")
#     return config


MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

def write_log(
    log_df, 
    log_type: str,               # 'task' or 'pipeline'
    environment: str = "dev",
    partition_cols: list = None,
    file_format: str = "delta"
):
    """
    Retry-safe write to Delta or Parquet, auto-creating the table if it doesn't exist.

    Parameters:
        log_df (DataFrame): Spark DataFrame to write
        path (str): target path
        partition_cols (list): columns to partition by
        file_format (str): "delta" (default) or "parquet"
    """

    partition_cols = partition_cols or [] 
    # 1. Resolve path from config
    path = get_log_config(log_type, environment=environment)

    # 2. Prepare writer
    writer = log_df.write.mode("append").partitionBy(*partition_cols)

    # 3️. Retry-safe write
    attempt = 0

    while attempt < MAX_RETRIES:
        try:
            if file_format.lower() == "delta":
                writer.format("delta").save(path)
            elif file_format.lower() == "parquet":
                writer.format("parquet").save(path)
            else:
                raise ValueError(f"Unsupported format: {file_format}")
            return path  # success
        except Exception as e:
            attempt += 1
            print(f"Write attempt {attempt} failed: {e}")
            time.sleep(RETRY_DELAY)
    
    raise RuntimeError(f"Failed to write logs after {MAX_RETRIES} attempts")

# --------------------------
# Convenience wrappers
# --------------------------
def write_task_log(log_df, environment="dev", partition_cols=None, file_format="delta"):
    """Convenience wrapper for task logs"""
    return write_log(
        log_df, 
        log_type="task", 
        environment=environment, 
        partition_cols=partition_cols, 
        file_format=file_format
    )

def write_pipeline_log(log_df, environment="dev", partition_cols=None, file_format="delta"):
    """Convenience wrapper for pipeline logs"""
    return write_log(
        log_df, 
        log_type="pipeline", 
        environment=environment, 
        partition_cols=partition_cols, 
        file_format=file_format
    )



# def save_to_gold_with_log(tables: dict, log_table: str = "gold.save_status_log"):
#     """
#     Save Spark DataFrames to Gold layer Delta tables with error handling and logging.
#     Creates or appends to a log Delta table.
#     """
#     logs = []

#     for table_name, df in tables.items():
#         try:
#             df.write.mode("overwrite").saveAsTable(f"gold.{table_name}")
#             status = "SUCCESS"
#             error_message = None
#             print(f"SUCCESS: gold.{table_name} saved.")
#         except Exception as e:
#             status = "FAILED"
#             error_message = str(e)
#             print(f"ERROR saving gold.{table_name}: {error_message}")

#         logs.append({
#             "table_name": table_name,
#             "status": status,
#             "error_message": error_message,
#             "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
#         })

#     # Convert logs to DataFrame
#     logs_df = spark.createDataFrame(logs)

#     # Append logs to Delta table (create if not exists)
#     logs_df.write.mode("append").saveAsTable(log_table)

#     print(f"Status log updated at {log_table}")

# save_to_gold_with_log(gold_tables)

# SELECT * FROM gold.save_status_log ORDER BY timestamp DESC;

# def save_to_gold(tables: dict):
#     """
#     Save Spark DataFrames to Gold layer Delta tables with error handling.
#     """
#     for table_name, df in tables.items():
#         try:
#             df.write.mode("overwrite").saveAsTable(f"gold.{table_name}")
#             print(f"SUCCESS: gold.{table_name} saved.")
#         except Exception as e:
#             print(f"ERROR saving gold.{table_name}: {e}")

# gold_tables = {
#     "daily_flight_summary": daily_summary,
#     "airline_performance": airline_performance,
#     "route_analysis": route_analysis,
#     "origin_airport_performance": origin_airport_stats,
#     "destination_airport_performance": dest_airport_stats,
#     "seasonal_flight_trends": seasonal_analysis,
#     "delay_distribution": delay_distribution,
#     "aircraft_utilization": aircraft_utilization,
#     "weekend_vs_weekday_performance": weekend_analysis,
#     # New additions
#     "top_10_busiest_routes": top_10_busiest_routes,
#     "airline_cancellation_rates": cancellation_rates_airline,
#     "avg_delay_by_airline_month": avg_delay_by_airline_month
# }

# save_to_gold_with_log(gold_tables)

# for table_name, df in gold_tables.items():
#     df.write.mode("overwrite").saveAsTable(f"gold.{table_name}")
#     print(f"Saved: gold.{table_name}")

# print("All analytics tables successfully created in Gold layer!")