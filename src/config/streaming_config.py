from dataclasses import dataclass
from typing import Optional
# from .environments_config import ENVIRONMENTS
from .table_config_utils import TableConfig

@dataclass
class StreamingConfig:
    """
    Configuration for streaming operations

    Centralizes streaming-related settings.
        - checkpoint_base_path: Base folder where checkpoints go.
        - output_mode: Default "append".
        - trigger_available_now: Boolean (true = process available data now).
        - processing_time: Optional trigger interval.

    Method:
        get_checkpoint_path() builds a unique path per table and operation.

    
    """
    checkpoint_base_path: str
    output_mode: str = "append"
    trigger_available_now: bool = True
    processing_time: Optional[str] = None
    
    def get_checkpoint_path(self, table_config: TableConfig, operation: str = "write") -> str:
        """Generate checkpoint path for a specific table and operation"""
        return f"{self.checkpoint_base_path}/{table_config.catalog}/{table_config.schema}/{table_config.table}/{operation}"


def get_streaming_config(environment: str = "dev") -> StreamingConfig:
    """
    Get streaming configuration for a specific environment.
    
    Parameters:
    - environment: Environment (dev, staging, prod)
    
    Returns:
    - StreamingConfig instance
    """
    env_config = ENVIRONMENTS[environment]
    
    return StreamingConfig(
        checkpoint_base_path=env_config["checkpoint_base"],
        output_mode="append",
        trigger_available_now=True
    )


# Example usage:
# # Get silver table in prod
# silver_prod = get_table_config("silver", "prod")
# print(silver_prod.full_name)  
# # "store1_prod.silver.dimestore_silver"