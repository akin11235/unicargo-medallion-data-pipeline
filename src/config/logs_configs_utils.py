import os
import sys
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
import yaml

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

# Add src folder to sys.path safely (idempotent)
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))


# --------------------------
# Compute project root
# --------------------------
# Adjust the number of levels up to reach project root
# Your structure: unicargo_dab/src/config/ -> go up 2 levels
PROJECT_ROOT = BASE_DIR.parents[1]


# --------------------------
# Load logs_storage.yaml
# --------------------------
LOGS_CONFIG_PATH = PROJECT_ROOT / "configs" / "logs_storage.yaml"

try:
    # Load YAML once at module load
    with open(LOGS_CONFIG_PATH, "r") as f:
        LOGS_CONFIG = yaml.safe_load(f)["logs"]
except FileNotFoundError:
    raise FileNotFoundError(f"logs_storage.yaml not found at {LOGS_CONFIG_PATH}")
except Exception as e:
    raise RuntimeError(f"Error loading logs_storage.yaml: {e}")


# --------------------------
# Helper function
# --------------------------
def get_log_adls_path(log_type: str, environment: str = "dev") -> str:
    """
    Get full ADLS path for logs (task or pipeline) from logs_storage.yaml.
    
    Args:
        log_type (str): 'task' or 'pipeline'
        environment (str): 'dev', 'staging', or 'prod'
    
    Returns:
        Full path string
    """
    env_config = LOGS_CONFIG.get(environment)
    
    if not env_config:
        raise ValueError(
            f"No log config found for environment '{environment}'. "
            f"Valid options: {list(LOGS_CONFIG.keys())}"
        )
    
    base = env_config["base_path"]

    if log_type == "task":
        return f"{base}/{env_config['task_table']}"
    elif log_type == "pipeline":
        return f"{base}/{env_config['pipeline_table']}"
    else:
        raise ValueError("log_type must be 'task' or 'pipeline'")
