# --- Task 1: Read (with simulated failure)
try:
    # Simulate a failure by pointing to a non-existent file
    airlines_df = (
        spark.read
        .schema(airlines_schema)
        .option("header", "true")
        .csv("abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/raw/volumes/non_existent_file.csv")
    )

    rows_processed = airlines_df.count()

    # Log SUCCESS
    log_task_status(
        status="SUCCESS",
        rows=rows_processed,
        message="Airlines data read successfully",
        pipeline_name=pipeline_name,
        pipeline_id=None,
        file_format="delta"
    )

except Exception as e:
    # Log FAILURE
    try:
        log_task_status(
            status="FAILED",
            message=str(e),
            pipeline_name=pipeline_name,
            pipeline_id=None,
            file_format="delta"
        )
    except Exception as log_error:
        print(f"Failed to write task log: {log_error}")

    print("Simulated failure caught. Task log written.")
    # Optionally continue without raising
    # raise  # Uncomment to still crash the notebook



    # --- Task 1: Read (with simulated failure)

tasks = [
    {"name": "read_airlines", "path": "abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/raw/volumes/airlines.csv"},
    {"name": "read_invalid", "path": "abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/raw/volumes/missing_file.csv"},
    {"name": "read_flights", "path": "abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/raw/volumes/flights.csv"}
]

for task in tasks:
    try:
        print(f"Running task: {task['name']}")
        
        # Attempt to read CSV
        df = (
            spark.read
            .schema(airlines_schema)  # or flights_schema for other tasks
            .option("header", "true")
            .csv(task["path"])
        )
        rows_processed = df.count()
        
        # Log success
        log_task_status(
            status="SUCCESS",
            rows=rows_processed,
            operation=f"{task['name']} completed successfully",
            pipeline_name=pipeline_name,
            pipeline_id=None,
            file_format="delta"
        )
        
    except Exception as e:
        # Log failure but continue
        log_task_status(
            status="FAILED",
            operation=str(e),
            pipeline_name=pipeline_name,
            pipeline_id=None,
            file_format="delta"
        )
        print(f"Task {task['name']} failed: {e}")
        # Do NOT raise, continue to next task




tasks = [
    {"name": "read_airlines", "path": "abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/raw/volumes/airlines.csv", "fail_transform": False},
    {"name": "read_flights", "path": "abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/raw/volumes/flights.csv", "fail_transform": True}
]

for task in tasks:
    try:
        print(f"Running task: {task['name']}")
        
        # Read CSV
        df = (
            spark.read
            .schema(airlines_schema)  # or flights_schema for flights
            .option("header", "true")
            .csv(task["path"])
        )
        
        # Simulate a runtime error during transformation
        if task.get("fail_transform"):
            # Example: divide by zero or invalid operation
            df = df.withColumn("simulate_error", df["iata_code"] / 0)
        
        rows_processed = df.count()
        
        # Log success
        log_task_status(
            status="SUCCESS",
            rows=rows_processed,
            message=f"{task['name']} completed successfully",
            pipeline_name=pipeline_name,
            pipeline_id=None,
            file_format="delta"
        )
        
    except Exception as e:
        # Log failure but continue
        log_task_status(
            status="FAILED",
            message=str(e),
            pipeline_name=pipeline_name,
            pipeline_id=None,
            file_format="delta"
        )
        print(f"Task {task['name']} failed: {e}")
        # Continue to next task without stopping notebook


tasks = [
    {"name": "read_airlines", "path": "abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/raw/volumes/airlines.csv", "fail_transform": False},
    {"name": "read_flights", "path": "abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/raw/volumes/flights.csv", "fail_transform": True}
]

for task in tasks:
    try:
        print(f"Running task: {task['name']}")
        
        # Read CSV
        df = (
            spark.read
            .schema(airlines_schema)  # adjust schema per dataset
            .option("header", "true")
            .csv(task["path"])
        )
        
        # Simulate a runtime error during transformation
        if task.get("fail_transform"):
            df = df.withColumn("simulate_error", df["iata_code"] / 0)
        
        rows_processed = df.count()
        
        # Log success
        log_task_status(
            status="SUCCESS",
            rows=rows_processed,
            message=f"{task['name']} completed successfully",
            pipeline_name=pipeline_name,
            pipeline_id=None,
            file_format="delta"
        )
        
    except Exception as e:
        # Log failure with rows_processed = 0
        try:
            log_task_status(
                status="FAILED",
                rows=0,  # set to 0 on failure
                message=str(e),
                pipeline_name=pipeline_name,
                pipeline_id=None,
                file_format="delta"
            )
        except Exception as log_error:
            print(f"Failed to write task log: {log_error}")
        
        print(f"Task {task['name']} failed: {e}")
        # Continue to next task
