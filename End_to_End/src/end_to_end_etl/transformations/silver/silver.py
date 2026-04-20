from pyspark import pipelines as dp
from pyspark.sql.functions import *


# Please edit the sample below


@dp.table
def silver():
    return (
        spark.readStream.table("bronze")
        .dropDuplicates(["tpep_pickup_datetime"])
        .withColumn("ingest_at", current_timestamp())   
    )
