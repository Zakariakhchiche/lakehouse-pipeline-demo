"""Bronze : ingestion brute du CSV SNCF, sans transformation, horodatée.

spark-submit jobs/bronze_ingest.py --lake /data/lake --run-date 2026-09-30
"""
import argparse
import urllib.request
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from common import DATASET_URL


def main(lake: str, run_date: str) -> None:
    landing = Path(lake) / "landing" / f"regularite_{run_date}.csv"
    landing.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(DATASET_URL, landing)

    spark = SparkSession.builder.appName("bronze_ingest").getOrCreate()
    df = (
        spark.read.option("header", True).option("sep", ";").option("encoding", "UTF-8")
        .csv(str(landing))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_date", F.lit(run_date))
        .withColumn("_source_file", F.lit(landing.name))
    )
    # Delta : historique des versions et relecture possible (time travel).
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"_run_date = '{run_date}'")
       .partitionBy("_run_date")
       .save(f"{lake}/bronze/regularite_tgv"))
    print(f"bronze: {df.count()} lignes ingérées")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--lake", required=True)
    p.add_argument("--run-date", required=True)
    a = p.parse_args()
    main(a.lake, a.run_date)
