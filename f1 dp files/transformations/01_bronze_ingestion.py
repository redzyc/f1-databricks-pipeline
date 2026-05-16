from pyspark import pipelines as dp
from pyspark.sql import functions as F

landing_zone_base = "/Volumes/formula1/bronze/raw_data"
files = dbutils.fs.ls(landing_zone_base)

def create_bronze_table(table_name):
  @dp.table(name=f"formula1.bronze.{table_name}")
  def bronze_layer():
    return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"/Volumes/formula1/bronze/metadata_vol/{table_name}/schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(f"{landing_zone_base}/{table_name}")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

for file in files:
  table_name = file.name.replace("/", "")
  create_bronze_table(table_name)


