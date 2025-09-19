from pyspark.sql.types import DoubleType, StructType, StructField, IntegerType, StringType


airports_schema = StructType([
    StructField("iata_code", StringType(), True),
    StructField("airline", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])