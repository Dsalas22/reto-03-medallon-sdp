from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


SCHEMA = StructType([
    StructField("evento_id", StringType()),
    StructField("aeronave_id", StringType()),
    StructField("pais_origen", StringType()),
    StructField("velocidad_kmh", DoubleType()),
])


@dp.table()
def vuelos_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SCHEMA)
        .load(spark.conf.get("lab.source_path"))
    )


@dp.table()
@dp.expect_or_drop("velocidad_valida", "velocidad_kmh IS NOT NULL AND velocidad_kmh >= 0")
def vuelos_silver():
    return spark.readStream.table("vuelos_bronze")


@dp.materialized_view()
def vuelos_gold():
    return spark.read.table("vuelos_silver").groupBy("pais_origen").agg(
        F.count("*").alias("eventos"),
        F.avg("velocidad_kmh").alias("velocidad_promedio"),
    )
