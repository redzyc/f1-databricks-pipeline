from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.view(name="clean_circuits_view")
def clean_circuits_view():
    return (
        spark.readStream.table("formula1.bronze.circuits")
        .dropna(subset=["circuitId"])
    )
dp.create_streaming_table(name="formula1.silver.circuits")
dp.apply_changes(
    target="formula1.silver.circuits",
    source="clean_circuits_view",
    keys=["circuitId"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=1,
    ignore_null_updates=False
)


