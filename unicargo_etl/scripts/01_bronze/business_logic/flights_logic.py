from pyspark.sql import DataFrame
# from utils import add_pipeline_metadata  # optional

def transform_flights_logic(df: DataFrame, **kwargs) -> DataFrame:
    # df = add_pipeline_metadata(
    #     df,
    #     pipeline_id=kwargs.get("pipeline_id"),
    #     run_id=kwargs.get("run_id"),
    #     task_id=kwargs.get("task_id")
    # )
    # return df.filter(df.departure_delay > 0)
    return df