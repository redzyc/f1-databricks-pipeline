from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.view(name="clean_status_view")
def clean_status_view():
    return (
        spark.readStream.table("formula1.bronze.status")
        .dropna(subset=["statusId"])
    )
dp.create_streaming_table(name="formula1.silver.status")
dp.apply_changes(
    target="formula1.silver.status",
    source="clean_status_view",
    keys=["statusId"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=1,
    ignore_null_updates=False
)

