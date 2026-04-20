from pyspark import pipelines as dp
from pyspark.sql.functions import *


# Please edit the sample below


@dp.table()
def bronze():
    df = spark.readStream.table("samples.nyctaxi.trips")
    return df
