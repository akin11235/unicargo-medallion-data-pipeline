import os
import sys
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
import yaml
# from .environments_config import ENVIRONMENTS


"""
Configuration management for tables and data processing
"""

# --------------------------
# Determine base directory
# --------------------------
try:
    # If running as a Python script, __file__ is defined
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    # If running in a notebook, __file__ is not defined
    # Use current working directory as a fallback
    BASE_DIR = Path.cwd()

# --------------------------
# Compute project root
# --------------------------
# Adjust the number of levels up to reach project root
# Your structure: unicargo_dab/src/config/ -> go up 2 levels
PROJECT_ROOT = BASE_DIR.parents[1]

# --------------------------
# Path to yaml files
# --------------------------
TABLES_CONFIG_PATH = PROJECT_ROOT / "configs" / "tables.yaml"
ENVIRONMENTS_CONFIG_PATH = PROJECT_ROOT / "configs" / "environments.yaml"

# Add src folder to sys.path safely (idempotent)
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

# --------------------------
# Load environments YAML from project root
# --------------------------
try:
    with open(ENVIRONMENTS_CONFIG_PATH, "r") as f:
        ENVIRONMENTS_CONFIG = yaml.safe_load(f)["environments"]
    print(f"Loaded environments config from: {ENVIRONMENTS_CONFIG_PATH}")
except FileNotFoundError:
    print(f"YAML file not found at: {ENVIRONMENTS_CONFIG_PATH}")
    print(f"Current directory: {os.getcwd()}")
    print(f"Looking for: {os.path.abspath(ENVIRONMENTS_CONFIG_PATH)}")
    ENVIRONMENTS_CONFIG = {}
except Exception as e:
    print(f"Error loading YAML: {e}")
    ENVIRONMENTS_CONFIG = {}

# --------------------------
# Load tales YAML from project root
# --------------------------
try:
    # Load YAML once at module load
    with open(TABLES_CONFIG_PATH, "r") as f:
        TABLES_CONFIG = yaml.safe_load(f)["unikargo_tables"]
    print(f"Loaded tables config from: {TABLES_CONFIG_PATH}")
except FileNotFoundError:
    print(f"YAML file not found at: {TABLES_CONFIG_PATH}")
    print(f"Current directory: {os.getcwd()}")
    print(f"Looking for: {os.path.abspath(TABLES_CONFIG_PATH)}")
    TABLES_CONFIG = {}
except Exception as e:
    print(f"Error loading YAML: {e}")
    TABLES_CONFIG = {}


# -------------------------------
# TableConfig dataclass
# -------------------------------
@dataclass
class TableConfig:
    """
    Configuration for a table

    Represents a single table definition. 
    Holds all metadata needed to reference a table in Databricks / Unity Catalog:

    Parameters:
        catalog: Unity Catalog catalog (e.g. unikargo_dev).
        schema: schema name, usually prefixed for medallion layers (01_bronze)
        table: Table name (from YAML)
        layer: Logical layer (bronze, silver, gold)
        table_key: Only for gold tables (e.g., "daily_performance")
        raw_path: only for bronze tables (path to raw CSV)
        format: Defaults to "delta"

    Method:
    full_name → fully qualified table name (catalog.schema.table 
        -> "unikargo_dev.01_bronze.unikargo_airline_bronze").
    
    """
    catalog: str
    schema: str                 # numeric schema name, e.g., 01_bronze
    table: str
    layer: str                  # logical layer name, e.g., bronze
    table_key: Optional[str] = None
    format: str = "delta"
    raw_path: Optional[str] = None
    
    @property
    def full_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


def get_table_config(
        entity: str, 
        layer: str, 
        environment: str = "dev",
        table_key: Optional[str] = None
) -> TableConfig:
    """
    Get table configuration for a specific layer and environment.

    Args:
        entity: 'airlines', 'airports', 'flights'
        layer: Data layer: 'bronze', 'silver', 'gold'
        environment: 'dev', 'staging', 'prod'
        table_key: required for gold layer
    
    Returns:
    - TableConfig instance
    """
    env_config = ENVIRONMENTS_CONFIG[environment]  #Looks up the correct catalog in ENVIRONMENTS
    catalog = env_config["catalog"]
    prefix_map = env_config.get("prefix_map", {}) # Get numeric schema from environment prefix map
    schema = prefix_map.get(layer.lower())

    if not schema:
        raise ValueError(f"No schema prefix defined for layer '{layer}' in environment '{environment}'")


    entity_dict = TABLES_CONFIG.get(entity)
    if not entity_dict:
        raise KeyError(f"No tables defined for entity '{entity}'")


    # Gold tables require a table key
    if layer.lower() == "gold":
        if not table_key:
            raise ValueError("table_key is required for gold layer tables")
        table_name = entity_dict.get("gold", {}).get(table_key)
        if not table_name:
            raise KeyError(f"No gold table '{table_key}' defined for entity '{entity}'")
        raw_path = None

    else:
        table_name = entity_dict.get(layer.lower())
        if not table_name:
            raise KeyError(f"No table defined for entity '{entity}', layer '{layer}'")
        # Fetch raw path only for bronze tables
        raw_paths_dict = TABLES_CONFIG.get("raw_paths", {})
        raw_path = raw_paths_dict.get(entity) if layer.lower() == "bronze" else None
        

    return TableConfig(
        catalog=catalog,
        schema=schema,
        table=table_name,
        layer=layer.lower(),
        table_key=table_key,
        raw_path=raw_path
    )
