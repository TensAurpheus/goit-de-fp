from pyspark.sql.functions import avg, current_timestamp
from pyspark.sql import SparkSession
import os
import uuid
os.environ['PYSPARK_PYTHON'] = r'C:\ProgramData\anaconda3\envs\env_DE\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\ProgramData\anaconda3\envs\env_DE\python.exe'


# Ініціалізація SparkSession
spark = SparkSession.builder \
    .appName("Silver to Gold") \
    .getOrCreate()

# Завантаження Silver
athlete_bio_df = spark.read.parquet("silver/athlete_bio")
athlete_event_results_df = spark.read.parquet("silver/athlete_event_results")

# Join таблиць
joined_df = athlete_bio_df.drop("country_noc").join(athlete_event_results_df, on="athlete_id")

# Розрахунок середніх значень
gold_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("weight").alias("avg_weight"),
        avg("height").alias("avg_height")
) \
    .withColumn("timestamp", current_timestamp())

gold_df.show()
# Запис у Gold
gold_df.write.mode("overwrite").parquet("gold/avg_stats")
