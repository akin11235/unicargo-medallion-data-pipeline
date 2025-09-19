import uuid
import sys, argparse

# Global parser to avoid recreating it multiple times
_global_parser = None

def _get_widget(name, default=""):
    """
    Safe wrapper for dbutils.widgets.get:
    returns default if dbutils or widget does not exist.
    """
    try:
        import dbutils
        return dbutils.widgets.get(name)
    except Exception:
        return default


def _get_param(index, widget_name, default=None, spark=None):
    """
    Get a parameter for the orchestrator.
    Priority:
        1. sys.argv[index] if provided
        2. dbutils.widgets.get(widget_name) if in Databricks
        3. fallback default
    """
    # Try sys.argv
    try:
        return sys.argv[index]
    except IndexError:
        pass

    # Try Databricks widgets
    if spark is not None:
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            return dbutils.widgets.get(widget_name)
        except Exception:
            pass

    # Fallback default
    return default


# -----------------------
# Helpers
# -----------------------
def new_run_id():
    """Generate a unique run_id for a pipeline execution."""
    return str(uuid.uuid4())
