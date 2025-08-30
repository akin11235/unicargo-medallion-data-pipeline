from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from datetime import datetime

def save_with_log(df: DataFrame, table_name: str) -> None:
    """
    Saves a dataframe to a target Delta table and logs the operation.

    Parameters
    ----------
    df : DataFrame
        The dataframe to save.
    table_name : str
        Fully qualified name of the target table (catalog.schema.table).
    """
    # Fully qualified log table (production standard)
    log_table: str = "ops.logs.save_status_log"

    try:
        # Save the dataframe to target table
        df.write.mode("overwrite").saveAsTable(table_name)

        # Log success
        logs_df = spark.createDataFrame(
            [(table_name, "SUCCESS", datetime.now().isoformat())],
            ["table_name", "status", "timestamp"]
        )

    except Exception as e:
        # Log failure
        logs_df = spark.createDataFrame(
            [(table_name, f"FAILURE: {str(e)}", datetime.now().isoformat())],
            ["table_name", "status", "timestamp"]
        )
        raise

    finally:
        # Append logs to the dedicated log table
        logs_df.write.mode("append").saveAsTable(log_table)

# # Example Usage
# save_with_log(df_books, "main.gold.books")


# -- Create catalog for operational data (if not already there)
# CREATE CATALOG IF NOT EXISTS ops;

# -- Create schema for logs
# CREATE SCHEMA IF NOT EXISTS ops.logs;

# -- Create log table
# CREATE TABLE IF NOT EXISTS ops.logs.save_status_log (
#     table_name STRING,
#     status STRING,
#     timestamp TIMESTAMP
# ) USING DELTA;
