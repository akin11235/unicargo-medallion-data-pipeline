import uuid

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
    
# -----------------------
# Helpers
# -----------------------
def new_run_id():
    """Generate a unique run_id for a pipeline execution."""
    return str(uuid.uuid4())
