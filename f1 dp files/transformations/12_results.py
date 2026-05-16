from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.view(name="clean_results_view")
def clean_results_view():
    return (
        spark.readStream.table("formula1.bronze.results")
        .dropna(subset=["resultId"])
    )
dp.create_streaming_table(name="formula1.silver.results")
dp.apply_changes(
    target="formula1.silver.results",
    source="clean_results_view",
    keys=["resultId"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=1,
    ignore_null_updates=False
)


