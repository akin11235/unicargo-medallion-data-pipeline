import sys
from pathlib import Path

def add_src_to_path(levels_up=1, src_folder="src"):
    """
    Adds the src folder to sys.path reliably, works in scripts and notebooks.
    Only adds the path if it's not already in sys.path.

    Args:
        levels_up (int): How many levels to go up from current file/working dir to reach project root.
        src_folder (str): Name of the source folder to add.
    """
    base = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    src_path = base.parents[levels_up - 1] / src_folder
    src_str = str(src_path)
    if src_str not in sys.path:
        sys.path.append(src_str)
