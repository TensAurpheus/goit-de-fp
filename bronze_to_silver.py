from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace
import os
import uuid
os.environ['PYSPARK_PYTHON'] = r'C:\ProgramData\anaconda3\envs\env_DE\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\ProgramData\anaconda3\envs\env_DE\python.exe'

# Ініціалізація SparkSession
spark = SparkSession.builder \
    .appName("Bronze to Silver") \
    .getOrCreate()

# Функція чистки тексту


def clean_text(df):
    for column in df.columns:
        if df.schema[column].dataType.typeName() == "string":
            df = df.withColumn(column, regexp_replace(
                col(column), r'[^a-zA-Z0-9,.\\"\']', ""))
    return df


# Завантаження Bronze
athlete_bio_df = spark.read.parquet("bronze/athlete_bio")
athlete_event_results_df = spark.read.parquet("bronze/athlete_event_results")

# Чистка тексту та дедублікація
athlete_bio_cleaned = clean_text(athlete_bio_df).dropDuplicates()
athlete_event_results_cleaned = clean_text(
    athlete_event_results_df).dropDuplicates()

athlete_bio_cleaned.show()
athlete_event_results_cleaned.show()

# Запис у Silver
athlete_bio_cleaned.write.mode("overwrite").parquet("silver/athlete_bio")
athlete_event_results_cleaned.write.mode(
    "overwrite").parquet("silver/athlete_event_results")
