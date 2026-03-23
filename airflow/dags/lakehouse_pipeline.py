from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from src.pipeline.ingest_landing import ingest_landing
from src.pipeline.ingest_bronze import ingest_bronze
from src.pipeline.ingest_silver import ingest_silver
from src.pipeline.ingest_gold import ingest_gold
from src.pipeline.run_analytics import run_analytics

default_args = {
    "owner": "creiton-monteiro",
    "start_date": days_ago(1),
    "retries": 1,
}

with DAG(
    dag_id="lakehouse_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    landing = PythonOperator(
        task_id="landing",
        python_callable=ingest_landing
    )

    bronze = PythonOperator(
        task_id="bronze",
        python_callable=ingest_bronze
    )

    silver = PythonOperator(
        task_id="silver",
        python_callable=ingest_silver
    )

    gold = PythonOperator(
        task_id="gold",
        python_callable=ingest_gold
    )

    analytics = PythonOperator(
        task_id="analytics",
        python_callable=run_analytics
    )

    landing >> bronze >> silver >> gold >> analytics
