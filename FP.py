import logging
from configs import kafka_config
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, when, to_json, from_json, struct, lit, avg, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DoubleType, FloatType


import os
import uuid
os.environ['PYSPARK_PYTHON'] = r'C:\ProgramData\anaconda3\envs\env_DE\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\ProgramData\anaconda3\envs\env_DE\python.exe'
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_bio"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .appName("JDBC") \
    .getOrCreate()

# 1. Read athlete bio data from SQL database
df_athlete_bio = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
    dbtable=jdbc_table,
    user=jdbc_user,
    password=jdbc_password) \
    .load()

df_athlete_bio.show()

# 2. Filter athlete bio data
df_athlete_bio_filtered = df_athlete_bio.filter((col("height").cast(
    FloatType()).isNotNull()) & (col("weight").cast(FloatType()).isNotNull()))

df_athlete_bio_filtered.show()

# 3a. Read athlete event data from MySQL table

jdbc_table = "athlete_event_results"

athlete_event_results_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable=jdbc_table,
    user=jdbc_user,
    password=jdbc_password) \
    .load()

athlete_event_results_df.show()

# 3b. Stream to kafka and read from kafka
# athlete_event_results_df.selectExpr(
#     "CAST(to_json(struct(*)) AS STRING) as value").show(truncate=False)

# athlete_event_results_df \
#     .selectExpr("CAST(to_json(struct(*)) AS STRING) as value") \
#     .write \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
#     .option("kafka.security.protocol", kafka_config['security_protocol']) \
#     .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
#     .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_config['username']}\" password=\"{kafka_config['password']}\";") \
#     .option("topic", "rudyi_athlete_event_results") \
#     .option("kafka.metadata.max.age.ms", "120000") \
#     .save()


# # Read from Kafka
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
#     .option("kafka.security.protocol", kafka_config['security_protocol']) \
#     .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
#     .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_config['username']}\" password=\"{kafka_config['password']}\";") \
#     .option("subscribe", "rudyi_athlete_event_results") \
#     .option("kafka.metadata.max.age.ms", "120000") \
#     .load()

# json_schema = StructType([
#     StructField("edition", StringType(), True),
#     StructField("edition_id", IntegerType(), True),
#     StructField("country_noc", StringType(), True),
#     StructField("sport", StringType(), True),
#     StructField("event", StringType(), True),
#     StructField("result_id", IntegerType(), True),
#     StructField("athlete", StringType(), True),
#     StructField("athlete_id", IntegerType(), True),
#     StructField("pos", StringType(), True),
#     StructField("medal", StringType(), True),
#     StructField("isTeamSport", StringType(), True)
# ])

# kafka_df.show() 

# kafka_df = kafka_df.withColumn("value_json", from_json(col("json_value"), json_schema))

# kafka_df.writeStream \
#     .trigger(processingTime="5 seconds") \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start() \
#     .awaitTermination()

# 4. Join athlete bio and event results
combined_df = athlete_event_results_df.drop("country_noc").join(
    df_athlete_bio_filtered, "athlete_id")

combined_df.show()

# 5. Calculate average height and weight
aggregated_df = combined_df \
    .groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ) \
    .withColumn("timestamp", current_timestamp())

aggregated_df.show()

# 6a. Stream data to Kafka topic

# kafka_df = aggregated_df.selectExpr("to_json(struct(*)) as value")
# kafka_df.show()

# jdbc_url = "jdbc:mysql://217.61.57.46:3306/artem_r"
# jdbc_table = "athlete_stats"

# query_to_kafka = aggregated_df \
#     .selectExpr("to_json(struct(*)) as value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
#     .option("security.protocol", kafka_config['security_protocol']) \
#     .option("sasl.mechanism", kafka_config['sasl_mechanism']) \
#     .option("sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_config['username']}\" password=\"{kafka_config['password']}\";") \
#     .option("topic", "rudyi_athlete_results") \
#     .option("checkpointLocation", "/tmp/kafka-checkpoint") \
#     .start()

# 6b. Write data to MySQL
# query_to_mysql = aggregated_df \
#     .writeStream \
#     .foreachBatch(lambda batch_df, _: batch_df.write
#                   .jdbc(mysql_url, "rudyi_athlete_results", mode="append", properties=mysql_properties)) \
#     .option("checkpointLocation", "/tmp/mysql-checkpoint") \
#     .start()

# query_to_kafka.awaitTermination()
# query_to_mysql.awaitTermination()

spark.stop()
