from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.view(name="clean_seasons_view")
def clean_seasons_view():
    return (
        spark.readStream.table("formula1.bronze.seasons")
        .dropna(subset=["year"])
    )
dp.create_streaming_table(name="formula1.silver.seasons")
dp.apply_changes(
    target="formula1.silver.seasons",
    source="clean_seasons_view",
    keys=["year"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=1,
    ignore_null_updates=False
)


