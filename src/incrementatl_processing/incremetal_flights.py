def process_incremental_flights(date_str):
    """Process flights for specific date incrementally"""

    # Read new data for specific date
    new_flights = spark.read.csv(f"/mnt/datalake/raw/flights/date={date_str}/")

    # Merge with existing data
    from delta.tables import DeltaTable

    existing_table = DeltaTable.forPath(spark, "/mnt/datalake/silver/fact_flight")

    existing_table.alias("existing") \
        .merge(
            new_flights.alias("new"),
            "existing.FLIGHT_SK = new.FLIGHT_SK"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()