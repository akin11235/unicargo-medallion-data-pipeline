def get_external_location_path(spark, logical_name):
    try:
        path = spark.sql(f"DESCRIBE EXTERNAL LOCATION `{logical_name}`").select("url").collect()[0]["url"]
        print(f"{logical_name}_path: {path}", end="\n")
        return path
    except Exception as e:
        raise RuntimeError(f"Error resolving external location: {logical_name} → {str(e)}")