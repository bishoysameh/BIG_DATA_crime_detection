from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType
import pymysql


def insert_into_phpmyadmin(rows, database_name):

    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    # database = "big_data"
    username = "root"
    password = ""

    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database_name)
    cursor = conn.cursor()

    for row in rows:
        column1_value = row["original_crime_type_name"]
        column2_value_agg = row["crime_count"]
        print(f"Inserting: original_crime_type_name={column1_value}, crime_count={column2_value_agg}")
        sql_query1 = f"INSERT INTO crime_count (original_crime_type_name, crime_count) VALUES ('{column1_value}', {column2_value_agg})"
        cursor.execute(sql_query1)

    conn.commit()
    conn.close()


def insert_into_phpmyadmin_q2(rows, database_name):

    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    # database = "big_data"
    username = "root"
    password = ""

    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database_name)
    cursor = conn.cursor()

    for row in rows:
        column1_value = row["original_crime_type_name"]
        column2_value = row["city"]
        column2_value_agg = row["crime_count_forEach_city"]
        sql_query1 = f"INSERT INTO crime_count_foreachcity (original_crime_type_name, city,crime_count) VALUES ('{column1_value}', '{column2_value}', {column2_value_agg})"
        cursor.execute(sql_query1)

    conn.commit()
    conn.close()


def insert_into_phpmyadmin_q3(rows, database_name):

    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    # database = "big_data"
    username = "root"
    password = ""

    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database_name)
    cursor = conn.cursor()

    for row in rows:
        column1_value = row["original_crime_type_name"]
        column2_value = row["disposition"]
        column2_value_agg = row["disposition_count_foreach_crime"]
        sql_query1 = f"INSERT INTO disposition_count_foreach_crime (original_crime_type_name, disposition,disposition_count_foreach_crime) VALUES ('{column1_value}', '{column2_value}', {column2_value_agg})"
        cursor.execute(sql_query1)

    conn.commit()
    conn.close()


def insert_all_into_phpmyadmin(row):

    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    database = "big_data"
    username = "root"
    password = ""

    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    # Extract the required columns from the row
    column1_value = row.original_crime_type_name
    column2_value = row.report_date
    column3_value = row.call_date
    column4_value = row.call_time
    column5_value = row.disposition
    column6_value = row.address
    column7_value = row.city
    column8_value = row.state
    column9_value = row.address_type

    # Prepare the SQL query to insert data into the table
    sql_query = f"INSERT INTO big_data (original_crime_type_name, report_date, call_date, call_time, disposition, address, city, state, address_type) VALUES ('{column1_value}', '{column2_value}' , '{column3_value}', '{column4_value}' , '{column5_value}', '{column6_value}' , '{column7_value}', '{column8_value}','{column9_value}')"

    # Execute the SQL query
    cursor.execute(sql_query)

    # Commit the changes
    conn.commit()
    conn.close()


# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType().add("crime_id", IntegerType()).add("original_crime_type_name", StringType()).add("report_date", StringType()).add("call_date", StringType()).add("offense_date", StringType()).add("call_time", StringType()).add("call_date_time", StringType()).add("disposition", StringType()).add("address", StringType()).add("city", StringType()).add("state", StringType()).add("agency_id", IntegerType()).add("address_type", StringType()).add("common_location", StringType())


# Read data from Kafka topic as a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")


# aggregation of original_crime_type_name
df_aggregation = df.groupBy("original_crime_type_name").agg(count("*").alias("crime_count"))

# aggregation of original_crime_type_name for eachCity
df_crime_city = df.groupBy("original_crime_type_name", "city").agg(count("*").alias("crime_count_forEach_city")).filter(col("crime_count_forEach_city") > 1)

# aggregation of disposition foreach original_crime_type_name
df_crime_disposition = df.groupBy("original_crime_type_name", "disposition").agg(count("*").alias("disposition_count_foreach_crime")).filter(col("disposition_count_foreach_crime") > 1)


# .orderBy("count", ascending=False)


def process_row(batch_df, epoch_id):
    database_name = "big_data"

    # Collect the aggregated data into a list of rows
    rows = batch_df.collect()

    # Insert the aggregated data into MySQL with the determined database
    insert_into_phpmyadmin(rows, database_name)


def process_row_q2(batch_df, epoch_id):
    database_name = "big_data"

    # Collect the aggregated data into a list of rows
    rows = batch_df.collect()

    # Insert the aggregated data into MySQL with the determined database
    insert_into_phpmyadmin_q2(rows, database_name)


def process_row_q3(batch_df, epoch_id):
    database_name = "big_data"

    # Collect the aggregated data into a list of rows
    rows = batch_df.collect()

    # Insert the aggregated data into MySQL with the determined database
    insert_into_phpmyadmin_q3(rows, database_name)


query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreach(insert_all_into_phpmyadmin) \
    .start()


query1 = df_aggregation.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_row) \
    .start()

query2 = df_crime_city.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_row_q2) \
    .start()

query3 = df_crime_disposition.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_row_q3) \
    .start()

# Wait for the query to finish
query.awaitTermination()

query1.awaitTermination()

query2.awaitTermination()

query3.awaitTermination()

