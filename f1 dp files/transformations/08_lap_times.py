from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.view(name="clean_lap_times_view")
def clean_lap_times_view():
    return (
        spark.readStream.table("formula1.bronze.lap_times")
        .dropna(subset=["raceId", "driverId","lap"])
    )
dp.create_streaming_table(name="formula1.silver.lap_times")
dp.apply_changes(
    target="formula1.silver.lap_times",
    source="clean_lap_times_view",
    keys=["raceId", "driverId","lap"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=1,
    ignore_null_updates=False
)


