from pyspark.sql import DataFrame
from unikargo_utils import add_pipeline_metadata

def read_csv_data(spark, schema, config, **kwargs) -> DataFrame:
    """Generic CSV reader for any entity"""
    return(
        spark.read
        .schema(schema)
        .option("header", "true")
        .csv(config.raw_path))


# def transform_data(df, transform_func=None, **kwargs):
#     """Generic transform wrapper"""
#     if transform_func:
#         return transform_func(df)
#     return df  # No-op transform

def transform_data(df, transform_func=None, add_metadata=True, **kwargs):
    """Generic transform wrapper with optional metadata"""
    # Apply business transformation
    if transform_func:
        df = transform_func(df)
    
    # Add metadata if requested
    if add_metadata:
        df = add_pipeline_metadata(
            df,
            pipeline_id=kwargs.get("pipeline_id"),
            run_id=kwargs.get("run_id"),
            task_id=kwargs.get("task_id")
        )
    
    return df

def write_bronze_table(df: DataFrame, config, **kwargs):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.full_name)

# def write_bronze_table(df, config, **kwargs):
#     """Generic bronze layer writer for any entity"""
#     # Add pipeline metadata here (centralized!)
#     df_with_metadata = add_pipeline_metadata(
#         df,
#         pipeline_id=kwargs.get("pipeline_id"),
#         run_id=kwargs.get("run_id"),
#         task_id=kwargs.get("task_id")
#     )
    
#     df_with_metadata.write\
#         .mode("overwrite")\
#         .option("overwriteSchema", "true")\
#         .saveAsTable(config.full_name)