from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(name="formula1.gold.pit_stop_efficiency")
def gold_agg():
    pit_stops = spark.read.table("formula1.silver.pit_stops")
    drivers = spark.read.table("formula1.silver.drivers").withColumn("driver_full_name", F.concat(F.col("forename"), F.lit(" "), F.col("surname")))
    races = spark.read.table("formula1.silver.races").withColumn("race_name",F.col("name"))
    return(
        pit_stops.join(drivers, pit_stops.driverId == drivers.driverId).join(races, pit_stops.raceId == races.raceId).groupBy("driver_full_name","race_name")
        .agg(
            F.count(F.col("stop")).alias("number_of_pit_stops"),
            F.sum(F.col("duration")).alias("total_duration")
            )
    )


