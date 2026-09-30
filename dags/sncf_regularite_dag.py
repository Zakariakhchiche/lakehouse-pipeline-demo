"""DAG Airflow : bronze -> silver -> gold chaque mois, avec reprises automatiques."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

LAKE = "/opt/lake"
JOBS = "/opt/airflow/jobs"
SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}
PACKAGES = "io.delta:delta-spark_2.12:3.2.0"

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="sncf_regularite_tgv",
    description="Ponctualité TGV : lakehouse bronze / silver / gold",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 5 * *",  # le 5 de chaque mois, après publication des données AQST
    catchup=False,
    default_args=default_args,
    tags=["lakehouse", "open-data", "spark", "delta"],
) as dag:

    def spark_task(task_id: str, script: str, *args: str) -> SparkSubmitOperator:
        return SparkSubmitOperator(
            task_id=task_id,
            application=f"{JOBS}/{script}",
            py_files=f"{JOBS}/common.py",
            packages=PACKAGES,
            conf=SPARK_CONF,
            application_args=list(args),
            conn_id="spark_default",
        )

    bronze = spark_task("bronze_ingest", "bronze_ingest.py", "--lake", LAKE, "--run-date", "{{ ds }}")
    silver = spark_task("silver_clean", "silver_clean.py", "--lake", LAKE, "--run-date", "{{ ds }}")
    gold = spark_task("gold_kpis", "gold_kpis.py", "--lake", LAKE)

    bronze >> silver >> gold
