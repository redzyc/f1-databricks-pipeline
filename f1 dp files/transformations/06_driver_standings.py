from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.view(name="clean_driver_standings_view")
def clean_driver_standings_view():
    return (
        spark.readStream.table("formula1.bronze.driver_standings")
        .dropna(subset=["driverStandingsId"])
    )
dp.create_streaming_table(name="formula1.silver.driver_standings")
dp.apply_changes(
    target="formula1.silver.driver_standings",
    source="clean_driver_standings_view",
    keys=["driverStandingsId"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=1,
    ignore_null_updates=False
)

