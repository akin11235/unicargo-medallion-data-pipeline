"""Unity Catalog operations for managing catalogs, schemas, and tables."""
from pyspark.sql import SparkSession
from typing import Dict, Optional

class CatalogManager:
    """Manages Unity Catalog operations."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def create_and_select_catalog(self, catalog_name: str, managed_location: str) -> str:
        """Create catalog and switch to it."""
        existing_catalogs = [row.catalog for row in self.spark.sql("SHOW CATALOGS").collect()]
        
        if catalog_name in existing_catalogs:
            print(f"Catalog '{catalog_name}' already exists.")
        else:
            print(f"Creating catalog '{catalog_name}' at '{managed_location}'...")
            self.spark.sql(f"""
                CREATE CATALOG {catalog_name}
                MANAGED LOCATION '{managed_location}'
            """)
            print(f"Catalog '{catalog_name}' created.")
        
        # Switch to catalog
        self.spark.sql(f"USE CATALOG {catalog_name}")
        current_catalog_name = self.spark.sql("SELECT current_catalog()").collect()[0][0]
        print(f"Now using catalog: {current_catalog_name}")
        
        return current_catalog_name
    
    def create_schema(
        self, 
        catalog_name: str, 
        schema_name: str, 
        managed_location: str,
        dry_run: bool = False
    ) -> str:
        """Create schema in the specified catalog."""
        schema_name = schema_name.lower()
        
        if dry_run:
            print(f"DRY RUN: Would create schema `{catalog_name}.{schema_name}` at '{managed_location}'")
        else:
            print(f"Creating schema `{catalog_name}.{schema_name}`...")
            self.spark.sql(f"USE CATALOG '{catalog_name}'")
            self.spark.sql(f"""
                CREATE SCHEMA IF NOT EXISTS `{schema_name}`
                MANAGED LOCATION '{managed_location}'
            """)
            print(f"Schema `{schema_name}` created.")
        
        return schema_name


class TableManager:
    """Manages table creation and operations."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def create_bronze_table(self, catalog_name: str, schema_name: str, table_name: str) -> None:
        """Create bronze layer table."""
        print(f"Using catalog {catalog_name} and schema {schema_name}")
        self.spark.sql(f"USE CATALOG '{catalog_name}'")
        
        print(f"Creating table {catalog_name}.{schema_name}.{table_name}")
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
            id BIGINT,
            title STRING,
            price DOUBLE,
            description STRING,
            category STRING,
            image STRING,
            rating STRUCT<rate: DOUBLE, count: BIGINT>,
            ingestion_file STRING,
            ingestion_date TIMESTAMP
        )
        USING DELTA
        """)
    
    def create_silver_table(self, catalog_name: str, schema_name: str, table_name: str) -> None:
        """Create silver layer table."""
        print(f"Using catalog {catalog_name} and schema {schema_name}")
        self.spark.sql(f"USE CATALOG '{catalog_name}'")
        
        print(f"Creating table {catalog_name}.{schema_name}.{table_name}")
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
            id BIGINT,
            title STRING,
            price DOUBLE,
            category STRING,
            rating STRUCT<rate: DOUBLE, count: BIGINT>
        )
        USING DELTA
        """)
    
    def create_gold_table(self, catalog_name: str, schema_name: str, table_name: str) -> None:
        """Create gold layer table."""
        print(f"Using catalog {catalog_name} and schema {schema_name}")
        self.spark.sql(f"USE CATALOG '{catalog_name}'")
        
        print(f"Creating table {catalog_name}.{schema_name}.{table_name}")
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
            id BIGINT,
            title STRING,
            price DOUBLE,
            category STRING
        )
        USING DELTA
        """)