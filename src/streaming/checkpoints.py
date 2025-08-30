"""Checkpoint management for structured streaming."""
from typing import Dict

class CheckpointManager:
    """Manages streaming checkpoints."""
    
    @staticmethod
    def get_checkpoint_locations() -> Dict[str, str]:
        """Get checkpoint locations for each layer."""
        return {
            "bronze": "abfss://medallion@adlsdimestoresdev.dfs.core.windows.net/checkpoints/bronze/AppendCheckpointNew",
            "silver": "abfss://medallion@adlsdimestoresdev.dfs.core.windows.net/checkpoints/silver/AppendCheckpoint", 
            "gold": "abfss://medallion@adlsdimestoresdev.dfs.core.windows.net/checkpoints/gold/AppendCheckpoint"
        }
    
    @staticmethod
    def get_checkpoint_location(layer: str) -> str:
        """Get checkpoint location for specific layer."""
        locations = CheckpointManager.get_checkpoint_locations()
        return locations.get(layer.lower(), "")