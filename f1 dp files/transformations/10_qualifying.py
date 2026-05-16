from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.view(name="clean_qualifying_view")
def clean_qualifying_view():
    return (
        spark.readStream.table("formula1.bronze.qualifying")
        .dropna(subset=["qualifyId"])
    )
dp.create_streaming_table(name="formula1.silver.qualifying")
dp.apply_changes(
    target="formula1.silver.qualifying",
    source="clean_qualifying_view",
    keys=["qualifyId"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=1,
    ignore_null_updates=False
)