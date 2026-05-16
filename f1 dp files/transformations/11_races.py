from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.view(name="clean_races_view")
def clean_races_view():
    return (
        spark.readStream.table("formula1.bronze.races")
        .dropna(subset=["raceId"])
    )
dp.create_streaming_table(name="formula1.silver.races")
dp.apply_changes(
    target="formula1.silver.races",
    source="clean_races_view",
    keys=["raceId"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=1,
    ignore_null_updates=False
)


