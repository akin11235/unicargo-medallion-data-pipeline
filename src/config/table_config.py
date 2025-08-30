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


# -------------------------------
# Load YAML
# -------------------------------
# Get path to project root (go up from src/config/ to project root)
# Paths
# BASE_DIR = os.path.dirname(__file__)  # src/config/  #folder containing the current script (src/config/)
# PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))  # Go up 2 levels to project root
# TABLES_PATH = os.path.join(PROJECT_ROOT, "tables.yaml")

# # Current working directory (where notebook runs)
# BASE_DIR = os.getcwd()  # could be workspace path
# PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, '..', '..', '..'))  # adjust as needed
# TABLES_PATH = os.path.join(PROJECT_ROOT, "tables.yaml")


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
TABLES_PATH = PROJECT_ROOT / "tables.yaml"

# Add src folder to sys.path safely (idempotent)
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))


# Load YAML from project root
try:
    with open(TABLES_PATH, "r") as f:
        TABLES_CONFIG = yaml.safe_load(f)["unikargo_tables"]
    print(f"Loaded tables config from: {TABLES_PATH}")
except FileNotFoundError:
    print(f"YAML file not found at: {TABLES_PATH}")
    print(f"Current directory: {os.getcwd()}")
    print(f"Looking for: {os.path.abspath(TABLES_PATH)}")
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
    else:
        table_name = entity_dict.get(layer.lower())
        if not table_name:
            raise KeyError(f"No table defined for entity '{entity}', layer '{layer}'")
        

    return TableConfig(
        catalog=catalog,
        schema=schema,
        table=table_name,
        layer=layer.lower(),
        table_key=table_key
    )





# import os
# from dataclasses import dataclass
# from typing import Optional
# import yaml
# from .environments_config import ENVIRONMENTS


# """
# Configuration management for tables and data processing
# """

# # Load YAML from project root

# # Get path to project root (go up from src/config/ to project root)
# BASE_DIR = os.path.dirname(__file__)  # src/config/
# PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))  # Go up 2 levels to project root
# TABLES_PATH = os.path.join(PROJECT_ROOT, "tables.yaml")

# try:
#     with open(TABLES_PATH, "r") as f:
#         TABLES_CONFIG = yaml.safe_load(f)["unikargo_tables"]
#     print(f"Loaded tables config from: {TABLES_PATH}")
# except FileNotFoundError:
#     print(f"YAML file not found at: {TABLES_PATH}")
#     print(f"Current directory: {os.getcwd()}")
#     print(f"Looking for: {os.path.abspath(TABLES_PATH)}")
#     TABLES_CONFIG = {}
# except Exception as e:
#     print(f"Error loading YAML: {e}")
#     TABLES_CONFIG = {}

# @dataclass
# class TableConfig:
#     """
#     Configuration for a table

#     Represents a single table definition.

#     Parameters:
#         catalog: Unity Catalog catalog (e.g., store1_dev).
#         schema: Schema (bronze, silver, gold).
#         table: Table name (e.g., dimestore_bronze).
#         format: Defaults to "delta".

#     Method:
#     full_name → gives the fully qualified table name (catalog.schema.table).
    
#     """
#     catalog: str
#     schema: str
#     table: str
#     layer: str
#     table_key: Optional[str] = None
#     format: str = "delta"
    
#     @property
#     def full_name(self) -> str:
#         return f"{self.catalog}.{self.schema}.{self.table}"


# def get_table_config(
#         entity: str, 
#         layer: str, 
#         environment: str = "dev",
#         table_key: Optional[str] = None
# ) -> TableConfig:
#     """
#     Get table configuration for a specific layer and environment.
    
#     Parameters:
#     - entity: 'airlines', 'airports', 'flights', or '03_gold'
#     - layer: Data layer (bronze, silver, gold)
#     - environment: Environment (dev, staging, prod)
#     - table_key: for gold layer only (e.g. 'daily_flight_summary')
    
#     Returns:
#     - TableConfig instance
#     """
#     env_config = ENVIRONMENTS[environment]  #Looks up the correct catalog in ENVIRONMENTS
#     catalog = env_config["catalog"]

#     entity_dict = TABLES_CONFIG.get(entity)
#     if not entity_dict:
#         raise KeyError(f"No tables defined for entity '{entity}'")

#     if layer == "gold":
#         if not table_key:
#             raise ValueError("table_key is required for gold layer tables")
#         table_name = entity_dict.get("gold", {}).get(table_key)
#         if not table_name:
#             raise KeyError(f"No gold table '{table_key}' defined for entity '{entity}'")
#     else:
#         table_name = entity_dict.get(layer)
#         if not table_name:
#             raise KeyError(f"No table defined for entity '{entity}', layer '{layer}'")

#     return TableConfig(
#         catalog=catalog,
#         schema=layer,
#         table=table_name,
#         layer=layer,
#         table_key=table_key
#     )
    
    


# # Example configurations for your project
# DIMESTORE_TABLES = {
#     "bronze": TableConfig(
#         catalog="store1_dev",
#         schema="bronze",
#         table="dimestore_bronze"
#     ),
#     "silver": TableConfig(
#         catalog="store1_dev",
#         schema="silver",
#         table="dimestore_silver"
#     ),
#     "gold": TableConfig(
#         catalog="store1_dev",
#         schema="gold",
#         table="dimestore_gold"
#     )
# }

# The DIMESTORE_TABLES dict is essentially a shortcut / convenience 
# mapping for your layers (bronze, silver, gold).

# Where you would use it:

# 1️⃣ Quick lookup in small scripts / notebooks

# Instead of calling get_table_config() each time:

# # Old
# silver_table = get_table_config("silver", "dev")

# # Using DIMESTORE_TABLES
# silver_table = DIMESTORE_TABLES["silver"]

# Access fully qualified name
# print(silver_table.full_name)
# -> store1_dev.silver.dimestore_silver
