from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.remote(cluster_id="0814-082012-qadumiea").getOrCreate()
spark.sql("SELECT 1").show()