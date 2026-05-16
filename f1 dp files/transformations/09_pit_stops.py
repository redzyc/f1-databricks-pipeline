from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.view(name="clean_pit_stops_view")
def clean_pit_stops_view():
    return (
        spark.readStream.table("formula1.bronze.pit_stops")
        .dropna(subset=["raceId", "driverId","stop"])
    )
dp.create_streaming_table(name="formula1.silver.pit_stops")
dp.apply_changes(
    target="formula1.silver.pit_stops",
    source="clean_pit_stops_view",
    keys=["raceId", "driverId","stop"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=1,
    ignore_null_updates=False
)



