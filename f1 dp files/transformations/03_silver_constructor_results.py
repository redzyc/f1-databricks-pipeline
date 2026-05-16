from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.view(name="clean_constructor_results_view")
def clean_constructor_results_view():
    return (
        spark.readStream.table("formula1.bronze.constructor_results")
        .dropna(subset=["constructorResultsId"])
    )
dp.create_streaming_table(name="formula1.silver.constructor_results")
dp.apply_changes(
    target="formula1.silver.constructor_results",
    source="clean_constructor_results_view",
    keys=["constructorResultsId"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=1,
    ignore_null_updates=False
)

