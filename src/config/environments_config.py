
# config-driven ETL
# Environment-specific configurations
ENVIRONMENTS = {
    "dev": {
        "catalog": "unikargo_dev",
        "prefix_map": {"bronze": "01_bronze", "silver": "02_silver", "gold": "03_gold"},
        "checkpoint_base": "/mnt/dev/checkpoints"
    },
    "staging": {
        "catalog": "unikargo_staging",
        "prefix_map": {"bronze": "01_bronze", "silver": "02_silver", "gold": "03_gold"},
        "checkpoint_base": "/mnt/staging/checkpoints"
    },
    "prod": {
        "catalog": "unikargo_prod",
        "prefix_map": {"bronze": "01_bronze", "silver": "02_silver", "gold": "03_gold"},
        "checkpoint_base": "/mnt/prod/checkpoints"
    }
}