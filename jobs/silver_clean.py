"""Silver : typage, renommage, dédoublonnage et contrôles qualité.

spark-submit jobs/silver_clean.py --lake /data/lake --run-date 2026-09-30
"""
import argparse

from pyspark.sql import SparkSession, functions as F

from common import rename_map


def main(lake: str, run_date: str) -> None:
    spark = SparkSession.builder.appName("silver_clean").getOrCreate()
    raw = spark.read.format("delta").load(f"{lake}/bronze/regularite_tgv").where(F.col("_run_date") == run_date)

    mapping = rename_map(raw.columns)
    df = raw.select([F.col(f"`{src}`").alias(dst) for src, dst in mapping.items()])

    df = (
        df.withColumn("month", F.to_date(F.concat_ws("-", F.col("month"), F.lit("01"))))
          .withColumn("planned_trains", F.col("planned_trains").cast("int"))
          .withColumn("cancelled_trains", F.col("cancelled_trains").cast("int"))
          .withColumn("late_trains", F.col("late_trains").cast("int"))
          .withColumn("avg_delay_min", F.col("avg_delay_min").cast("double"))
          .withColumn("departure_station", F.initcap(F.trim("departure_station")))
          .withColumn("arrival_station", F.initcap(F.trim("arrival_station")))
          .dropDuplicates(["month", "departure_station", "arrival_station"])
    )

    # Contrôles qualité : une ligne invalide est isolée, pas silencieusement supprimée.
    valid = (
        F.col("month").isNotNull()
        & (F.col("planned_trains") > 0)
        & (F.col("cancelled_trains").between(0, F.col("planned_trains")))
        & (F.col("late_trains") >= 0)
    )
    df.where(~valid).write.format("delta").mode("append").save(f"{lake}/quarantine/regularite_tgv")
    clean = df.where(valid)

    clean.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{lake}/silver/regularite_tgv")
    print(f"silver: {clean.count()} lignes valides, {df.count() - clean.count()} en quarantaine")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--lake", required=True)
    p.add_argument("--run-date", required=True)
    a = p.parse_args()
    main(a.lake, a.run_date)
