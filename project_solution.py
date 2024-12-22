from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Initialize the DAG
with DAG(
    'multi_hop_datalake',
    default_args=default_args,
    description='ETL pipeline with Spark and Airflow',
    schedule_interval=None,
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    # Task 1: Landing to Bronze
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application='/opt/airflow/dags/landing_to_bronze.py',
        name='LandingToBronze',
        conn_id='spark_default',
        conf={'spark.master': 'local', 'spark.executor.memory': '1g'},
        verbose=True
    )

    # Task 2: Bronze to Silver
    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application='/opt/airflow/dags/bronze_to_silver.py',
        name='BronzeToSilver',
        conn_id='spark_default',
        conf={'spark.master': 'local', 'spark.executor.memory': '1g'},
        verbose=True
    )

    # Task 3: Silver to Gold
    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application='/opt/airflow/dags/silver_to_gold.py',
        name='SilverToGold',
        conn_id='spark_default',
        conf={'spark.master': 'local', 'spark.executor.memory': '1g'},
        verbose=True
    )

    # Define task dependencies
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
