from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.view(name="clean_constructor_standings_view")
def clean_constructor_standings_view():
    return (
        spark.readStream.table("formula1.bronze.constructor_standings")
        .dropna(subset=["constructorStandingsId"])
    )
dp.create_streaming_table(name="formula1.silver.constructor_standings")
dp.apply_changes(
    target="formula1.silver.constructor_standings",
    source="clean_constructor_standings_view",
    keys=["constructorStandingsId"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=1,
    ignore_null_updates=False
)

