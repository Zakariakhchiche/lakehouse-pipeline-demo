"""Gold : indicateurs de ponctualité par liaison et par mois, prêts pour Power BI.

spark-submit jobs/gold_kpis.py --lake /data/lake
"""
import argparse

from pyspark.sql import SparkSession, Window, functions as F


def main(lake: str) -> None:
    spark = SparkSession.builder.appName("gold_kpis").getOrCreate()
    s = spark.read.format("delta").load(f"{lake}/silver/regularite_tgv")

    ran = F.col("planned_trains") - F.col("cancelled_trains")
    kpis = (
        s.withColumn("route", F.concat_ws(" → ", "departure_station", "arrival_station"))
         .withColumn("punctuality_pct", F.round(100 * (ran - F.col("late_trains")) / ran, 2))
         .withColumn("cancellation_pct", F.round(100 * F.col("cancelled_trains") / F.col("planned_trains"), 2))
    )

    # Tendance : écart à la moyenne glissante sur 12 mois de la même liaison.
    w = Window.partitionBy("route").orderBy("month").rowsBetween(-11, 0)
    kpis = kpis.withColumn("punctuality_12m_avg", F.round(F.avg("punctuality_pct").over(w), 2)) \
               .withColumn("vs_12m_pts", F.round(F.col("punctuality_pct") - F.col("punctuality_12m_avg"), 2))

    (kpis.select("month", "route", "departure_station", "arrival_station", "planned_trains",
                 "punctuality_pct", "cancellation_pct", "avg_delay_min",
                 "punctuality_12m_avg", "vs_12m_pts")
         .write.format("delta").mode("overwrite").option("overwriteSchema", True)
         .save(f"{lake}/gold/punctuality_by_route"))

    # Classement des 10 liaisons les moins ponctuelles sur le dernier mois disponible.
    last = kpis.agg(F.max("month")).first()[0]
    (kpis.where(F.col("month") == last).orderBy("punctuality_pct").limit(10)
         .write.format("delta").mode("overwrite").save(f"{lake}/gold/worst_routes_last_month"))
    print(f"gold: KPI calculés jusqu'à {last}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--lake", required=True)
    main(p.parse_args().lake)
