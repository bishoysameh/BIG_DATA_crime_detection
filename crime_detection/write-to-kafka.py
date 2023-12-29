from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType().add("crime_id", IntegerType()).add("original_crime_type_name", StringType()).add("report_date", StringType()).add("call_date", StringType()).add("offense_date", StringType()).add("call_time", StringType()).add("call_date_time", StringType()).add("disposition", StringType()).add("address", StringType()).add("city", StringType()).add("state", StringType()).add("agency_id", IntegerType()).add("address_type", StringType()).add("common_location", StringType())


# Read data from a directory as a streaming DataFrame
streaming_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("path", "D:/Big_data_project/data") \
    .load() \

# Select specific columns from "data"
#df = streaming_df.select("name", "age")

#df = streaming_df.select(col("name").alias("key"), to_json(col("age")).alias("value"))
#df = streaming_df.select(to_json(struct("*")).alias("value"))


df = streaming_df.select(
col("original_crime_type_name").alias("key"),
to_json(struct("*")).alias("value")
#to_json(struct(col("name").alias("name"), col("age").alias("age"))).alias("value")
)



# Convert the value column to string and display the result
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "test") \
    .option("checkpointLocation", "null") \
    .start()

# Wait for the query to finish
query.awaitTermination()
