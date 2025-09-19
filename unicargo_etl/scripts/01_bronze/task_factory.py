from etl_operations import read_csv_data, transform_data, write_bronze_table

def create_entity_tasks(entity_name, spark, schema, config, transform_func=None, add_metadata=True):
    """Generic task factory for any entity"""
    return [
        {
            "entity": entity_name,
            "operation": "read",
            "func": lambda **kw: read_csv_data(spark, schema, config, **kw),
            "source_path": config.raw_path,
            "target_path": config.full_name
        },
        {
            "entity": entity_name,
            "operation": "transform",
            "func": lambda df, **kw: transform_data(df, transform_func, add_metadata, **kw),
            "source_path": config.raw_path,
            "target_path": config.full_name
        },
        {
            "entity": entity_name,
            "operation": "write",
            "func": lambda df, **kw: write_bronze_table(df, config, **kw),
            "source_path": config.raw_path,
            "target_path": config.full_name
        },
    ]