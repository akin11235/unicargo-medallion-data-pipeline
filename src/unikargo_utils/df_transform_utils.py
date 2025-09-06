from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, create_map, current_timestamp

def add_pipeline_metadata(
    df: DataFrame,
    pipeline_id: str,
    run_id: str,
    task_id: str
) -> DataFrame:
    """
    Adds a metadata column to a Spark DataFrame with pipeline info.

    Parameters:
        df (DataFrame): Input DataFrame
        pipeline_id (str): Pipeline identifier
        run_id (str): Run identifier
        task_id (str): Task identifier

    Returns:
        DataFrame: DataFrame with an additional 'metadata' column
    """
    return  df.withColumn(
    "metadata",
    create_map(
        lit("pipeline_id"), lit(pipeline_id),
        lit("run_id"), lit(run_id),
        lit("task_id"), lit(task_id)
    )
    ).withColumn("processed_timestamp", current_timestamp())