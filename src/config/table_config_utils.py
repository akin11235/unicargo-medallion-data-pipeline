import os
import sys
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
import yaml
from .environments_config import ENVIRONMENTS


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
# Path to tables.yaml
# --------------------------
TABLES_CONFIG_PATH = PROJECT_ROOT / "tables.yaml"

# Add src folder to sys.path safely (idempotent)
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))


# Load YAML from project root
try:
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

    Parameters:
        catalog: Unity Catalog catalog (e.g., store1_dev).
        schema: Prefixed schema (01_bronze, 02_silver, 03_gold).
        table: Table name (from YAML)
        layer: Logical layer (bronze, silver, gold)
        table_key: Only for gold tables
        format: Defaults to "delta"

    Method:
    full_name → fully qualified table name (catalog.schema.table).
    
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
    env_config = ENVIRONMENTS[environment]  #Looks up the correct catalog in ENVIRONMENTS
    catalog = env_config["catalog"]

    # Get numeric schema from environment prefix map
    prefix_map = env_config.get("prefix_map", {})
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


# --------------------------
# Load logs.yaml
# --------------------------
LOGS_CONFIG_PATH = PROJECT_ROOT / "logs.yaml" 

try:
    with open(LOGS_CONFIG_PATH, "r") as f:
        LOGS_CONFIG = yaml.safe_load(f)["logs"]
except FileNotFoundError:
    raise FileNotFoundError(f"logs.yaml not found at {LOGS_CONFIG_PATH}")
except Exception as e:
    raise RuntimeError(f"Error loading logs.yaml: {e}")


# --------------------------
# Helper function
# --------------------------
def get_log_config(log_type: str, environment: str = "dev") -> str:
    """
    Get full ADLS path for logs (task or pipeline) from logs.yaml.
    
    Args:
        log_type: 'task' or 'pipeline'
        environment: 'dev', 'staging', 'prod'
    
    Returns:
        Full path string
    """
    env_config = LOGS_CONFIG.get(environment)
    
    if not env_config:
        raise ValueError(f"No log config for environment '{environment}'")
    
    base = env_config["base_path"]
    if log_type == "task":
        return f"{base}/{env_config['task_table']}"
    elif log_type == "pipeline":
        return f"{base}/{env_config['pipeline_table']}"
    else:
        raise ValueError("log_type must be 'task' or 'pipeline'")
