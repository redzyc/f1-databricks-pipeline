from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.view(name="clean_constructors_view")
def clean_constructors_view():
    return (
        spark.readStream.table("formula1.bronze.constructors")
        .dropna(subset=["constructorId"])
    )
dp.create_streaming_table(name="formula1.silver.constructors")
dp.apply_changes(
    target="formula1.silver.constructors",
    source="clean_constructors_view",
    keys=["constructorId"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=1,
    ignore_null_updates=False
)



