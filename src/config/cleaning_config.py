from dataclasses import dataclass
from typing import List, Dict, Any, Optional


@dataclass
class CleaningConfig:
    """
    Configuration for data cleaning operations
    
    Defines rules for cleaning raw/bronze data before promoting it up the pipeline.

    Parameters:

        - columns_to_drop: Columns to discard.
        - string_fill_value: Fill value for null strings.
        - numeric_fill_value: Fill value for null numerics.
        - custom_fill_values: Dict for column-specific fills.
        - duplicate_subset: Which columns define duplicates (for .dropDuplicates()).
        - add_ingestion_timestamp: Whether to add ingestion_date.
        - timestamp_column: Column name for ingestion timestamp.
    """
    columns_to_drop: List[str]
    string_fill_value: str = "Unknown"
    numeric_fill_value: int = 0
    custom_fill_values: Optional[Dict[str, Any]] = None
    duplicate_subset: Optional[List[str]] = None
    add_ingestion_timestamp: bool = True
    timestamp_column: str = "ingestion_date"

# Predefined configs
BRONZE_CLEANING = CleaningConfig(columns_to_drop=[], add_ingestion_timestamp=True)
SILVER_CLEANING = CleaningConfig(columns_to_drop=["image", "description"], add_ingestion_timestamp=False)
GOLD_CLEANING = CleaningConfig(columns_to_drop=["ingestion_file", "ingestion_date"], add_ingestion_timestamp=False)

def get_cleaning_config(layer: str) -> CleaningConfig:
    """Return cleaning config per layer"""
    return {
        "bronze": BRONZE_CLEANING,
        "silver": SILVER_CLEANING,
        "gold": GOLD_CLEANING
    }.get(layer, BRONZE_CLEANING)


# Example usage
# Get cleaning rules for gold
# cleaning = get_cleaning_config("gold")
# print(cleaning.columns_to_drop)  
# # ["ingestion_file", "ingestion_date"]