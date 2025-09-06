"""Application constants."""

# Storage credentials
DEFAULT_STORAGE_CREDENTIAL = "db-dimestore-dev-storage-credential"

# Layer names
BRONZE_LAYER = "bronze"
SILVER_LAYER = "silver"
GOLD_LAYER = "gold"

# Schema mappings
LAYER_SCHEMA_MAP = {
    BRONZE_LAYER: BRONZE_LAYER,
    SILVER_LAYER: SILVER_LAYER,
    GOLD_LAYER: GOLD_LAYER
}