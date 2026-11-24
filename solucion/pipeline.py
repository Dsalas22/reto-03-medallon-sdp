import re

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


SUFFIX = spark.conf.get("pipelines.suffix", "").strip().lower()
if not re.fullmatch(r"[a-z]{3}", SUFFIX):
    raise ValueError("Configura pipelines.suffix con tres letras unicas, por ejemplo dsh.")

BRONZE = f"vuelos_bronze_{SUFFIX}"
SILVER = f"vuelos_silver_{SUFFIX}"
SCHEMA = StructType([
    StructField("evento_id", StringType()),
    StructField("aeronave_id", StringType()),
    StructField("pais_origen", StringType()),
    StructField("velocidad_kmh", DoubleType()),
    StructField("persona_id", StringType()),
    StructField("persona_alias", StringType()),
])


@dp.table(name=BRONZE)
def vuelos_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SCHEMA)
        .load(spark.conf.get("lab.source_path"))
    )


@dp.table(name=SILVER)
@dp.expect_or_drop("velocidad_valida", "velocidad_kmh IS NOT NULL AND velocidad_kmh >= 0")
def vuelos_silver():
    return spark.readStream.table(BRONZE)


@dp.materialized_view(name=f"vuelos_gold_{SUFFIX}")
def vuelos_gold():
    return spark.read.table(SILVER).groupBy("pais_origen").agg(
        F.count("*").alias("eventos"),
        F.avg("velocidad_kmh").alias("velocidad_promedio"),
    )
