from pyspark.sql.types import StructType, StructField, StringType

airlines_schema = StructType([
    StructField("iata_code", StringType(), True),
    StructField("airline", StringType(), True)
])
# 