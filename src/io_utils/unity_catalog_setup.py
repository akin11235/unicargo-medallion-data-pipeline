"""Unity Catalog infrastructure setup utilities."""
from pyspark.sql import SparkSession
from typing import Optional

class UnityCatalogSetup:
    """Handles Unity Catalog infrastructure setup."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def create_external_location(
        self, 
        name: str, 
        url: str, 
        storage_credential: str = "db-dimestore-dev-storage-credential", 
        comment: str = ""
    ) -> None:
        """Create external location if it doesn't exist."""
        # Check if external location already exists
        existing_locations = self.spark.sql("SHOW EXTERNAL LOCATIONS").toPandas()
        
        if name in existing_locations['name'].values:
            print(f"External location '{name}' already exists.")
            return
        
        # Construct and execute SQL
        create_sql = f"""
            CREATE EXTERNAL LOCATION IF NOT EXISTS `{name}`
            URL '{url}'
            WITH (CREDENTIAL `{storage_credential}`)
            COMMENT '{comment}'
        """
        
        self.spark.sql(create_sql)
        print(f"External location `{name}` created.")
    
    def setup_all_external_locations(self) -> None:
        """Create all required external locations."""
        locations = [
            ("metastore-products", 
             "abfss://dimestore-metastore@adlsdimestoresdev.dfs.core.windows.net/managed/dimestore_products_dev",
             "Metastore for products"),
            ("landing", 
             "abfss://medallion@adlsdimestoresdev.dfs.core.windows.net/landing",
             "External folder for landing"),
            ("bronze", 
             "abfss://medallion@adlsdimestoresdev.dfs.core.windows.net/bronze",
             "External store for bronze tables"),
            ("silver", 
             "abfss://medallion@adlsdimestoresdev.dfs.core.windows.net/silver",
             "External store for silver tables"),
            ("gold", 
             "abfss://medallion@adlsdimestoresdev.dfs.core.windows.net/gold",
             "External store for gold tables"),
            ("bronze_checkpointLocation",
             "abfss://medallion@adlsdimestoresdev.dfs.core.windows.net/checkpoints/bronze/AppendCheckpointNew",
             "External store for bronze checkpoint for structured streaming"),
            ("silver_checkpointLocation",
             "abfss://medallion@adlsdimestoresdev.dfs.core.windows.net/checkpoints/silver/AppendCheckpoint",
             "External store for silver checkpoint for structured streaming"),
            ("gold_checkpointLocation",
             "abfss://medallion@adlsdimestoresdev.dfs.core.windows.net/checkpoints/gold/AppendCheckpoint",
             "External store for gold checkpoint for structured streaming")
        ]
        
        for name, url, comment in locations:
            self.create_external_location(name, url, comment=comment)