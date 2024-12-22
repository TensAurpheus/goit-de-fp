import urllib.request
from pyspark.sql import SparkSession
import os
import uuid
os.environ['PYSPARK_PYTHON'] = r'C:\ProgramData\anaconda3\envs\env_DE\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\ProgramData\anaconda3\envs\env_DE\python.exe'


# Створення SparkSession
spark = SparkSession.builder \
    .appName("LandingToBronze") \
    .getOrCreate()

# Завантаження файлів з FTP
athlete_bio_url = "https://ftp.goit.study/neoversity/athlete_bio.csv"
athlete_event_results_url = "https://ftp.goit.study/neoversity/athlete_event_results.csv"

athlete_bio_local = 'athlete_bio.csv'
athlete_event_results_local = 'athlete_event_results.csv'

# Читання даних з CSV
urllib.request.urlretrieve(athlete_bio_url, athlete_bio_local)
urllib.request.urlretrieve(athlete_event_results_url,
                           athlete_event_results_local)

athlete_bio_df = spark.read.csv(
    athlete_bio_local, header=True, inferSchema=True)
athlete_event_results_df = spark.read.csv(
    athlete_event_results_local, header=True, inferSchema=True)

athlete_bio_df.show()
athlete_event_results_df.show()

# Запис даних у форматі Parquet
athlete_bio_df.write.mode("overwrite").parquet("bronze/athlete_bio")
athlete_event_results_df.write.mode(
    "overwrite").parquet("bronze/athlete_event_results")

spark.stop()
