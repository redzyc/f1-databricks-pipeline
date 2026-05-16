from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.view(name="clean_drivers_view")
def clean_drivers_view():
    return (
        spark.readStream.table("formula1.bronze.drivers")
        .dropna(subset=["driverId"])
    )
dp.create_streaming_table(name="formula1.silver.drivers")
dp.apply_changes(
    target="formula1.silver.drivers",
    source="clean_drivers_view",
    keys=["driverId"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=1,
    ignore_null_updates=False
)

