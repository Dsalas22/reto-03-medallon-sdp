# Reto 03: medallon completo. Bronce -> staging con calidad -> Data Vault (2 hubs, 1 link, 1 satelite) -> 2 vistas Oro.
import re

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


# ---------- Configuracion (Settings > Configuration) ----------
# Obligatorio: pipelines.suffix (tres letras). Opcionales, con estos valores por defecto:
# raw_catalog=raw_data, raw_schema=bronze, silver_catalog=data_vault, silver_schema=plata,
# gold_catalog=ods, gold_schema=gold, landing_volume=landing_retos_sdp
SUFFIX = spark.conf.get("pipelines.suffix", "").strip().lower()
if not re.fullmatch(r"[a-z]{3}", SUFFIX):
    raise ValueError("Configura pipelines.suffix con tres letras unicas, por ejemplo dsh.")


def config(clave, defecto):
    valor = spark.conf.get(clave, defecto).strip()
    if not re.fullmatch(r"[A-Za-z0-9_]+", valor):
        raise ValueError(f"Valor invalido para {clave}: '{valor}'. Usa solo letras, numeros y guion bajo.")
    return valor


RAW_CATALOG = config("raw_catalog", "raw_data")
RAW_SCHEMA = config("raw_schema", "bronze")
PLATA = f"{config('silver_catalog', 'data_vault')}.{config('silver_schema', 'plata')}"
ORO = f"{config('gold_catalog', 'ods')}.{config('gold_schema', 'gold')}"
VOLUMEN = config("landing_volume", "landing_retos_sdp")

RUTA_ENTRADA = f"/Volumes/{RAW_CATALOG}/{RAW_SCHEMA}/{VOLUMEN}/{SUFFIX}/reto03/entrada"

BRONCE = f"{RAW_CATALOG}.{RAW_SCHEMA}.r03_vuelos_raw_{SUFFIX}"
STAGING = f"r03_stg_vuelos_{SUFFIX}"
HUB_VUELO = f"{PLATA}.r03_hub_vuelo_{SUFFIX}"
HUB_AERONAVE = f"{PLATA}.r03_hub_aeronave_{SUFFIX}"
LINK_VUELO_AERONAVE = f"{PLATA}.r03_link_vuelo_aeronave_{SUFFIX}"
SAT_VUELO = f"{PLATA}.r03_sat_vuelo_{SUFFIX}"
POR_PAIS = f"{ORO}.r03_vuelos_por_pais_{SUFFIX}"
POR_AERONAVE = f"{ORO}.r03_vuelos_por_aeronave_{SUFFIX}"

SCHEMA = StructType([
    StructField("evento_id", StringType()),
    StructField("aeronave_id", StringType()),
    StructField("pais_origen", StringType()),
    StructField("velocidad_kmh", DoubleType()),
    StructField("persona_id", StringType()),
    StructField("persona_alias", StringType()),
])

# Atributos del satelite (la aeronave ya no es atributo: es otro hub unido por el link).
ATRIBUTOS = ["pais_origen", "velocidad_kmh", "persona_id", "persona_alias"]


def normalizar(columna):
    """Texto en mayusculas, sin espacios y sin nulos: base estable para calcular hashes."""
    return F.upper(F.trim(F.coalesce(F.col(columna).cast("string"), F.lit(""))))


# ---------- Bronce: conserva TODO, incluso los eventos invalidos ----------
@dp.table(name=BRONCE, comment="Bronce: eventos JSON tal como llegan, con archivo de origen y fecha de ingesta.")
def r03_vuelos_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SCHEMA)
        .load(RUTA_ENTRADA)
        .select(
            "*",
            F.col("_metadata.file_path").alias("archivo_origen"),
            F.col("_metadata.file_modification_time").alias("fecha_archivo"),
            F.current_timestamp().alias("fecha_ingesta"),
        )
    )


# ---------- Staging privado: regla de calidad + llaves hash ----------
@dp.temporary_view(name=STAGING, comment="Staging: descarta velocidades invalidas y calcula hash keys y hashdiff.")
@dp.expect_or_drop("velocidad_valida", "velocidad_kmh IS NOT NULL AND velocidad_kmh >= 0")
def r03_stg_vuelos():
    return spark.readStream.table(BRONCE).select(
        F.sha2(normalizar("evento_id"), 256).alias("hk_vuelo"),
        F.sha2(normalizar("aeronave_id"), 256).alias("hk_aeronave"),
        F.sha2(F.concat_ws("||", normalizar("evento_id"), normalizar("aeronave_id")), 256).alias("hk_vuelo_aeronave"),
        "evento_id",
        "aeronave_id",
        F.sha2(F.concat_ws("||", *[normalizar(c) for c in ATRIBUTOS]), 256).alias("hashdiff"),
        *ATRIBUTOS,
        F.col("fecha_archivo").alias("fecha_carga"),
        F.col("archivo_origen").alias("fuente_registro"),
    )


# ---------- Plata: hubs y link (SCD1 que conserva la PRIMERA carga de cada llave) ----------
# AUTO CDC se queda con la fila de mayor secuencia; con la fecha en negativo gana la carga mas antigua.
PRIMERA_CARGA = F.expr("-unix_micros(fecha_carga)")


def primera_carga_por_llave(destino, llave, columnas, comentario):
    dp.create_streaming_table(name=destino, comment=comentario)
    dp.create_auto_cdc_flow(
        target=destino,
        source=STAGING,
        keys=[llave],
        sequence_by=PRIMERA_CARGA,
        column_list=columnas + ["fecha_carga", "fuente_registro"],
        stored_as_scd_type=1,
    )


primera_carga_por_llave(HUB_VUELO, "hk_vuelo", ["hk_vuelo", "evento_id"], "Hub: una fila por evento_id.")
primera_carga_por_llave(HUB_AERONAVE, "hk_aeronave", ["hk_aeronave", "aeronave_id"], "Hub: una fila por aeronave_id.")
primera_carga_por_llave(
    LINK_VUELO_AERONAVE, "hk_vuelo_aeronave", ["hk_vuelo_aeronave", "hk_vuelo", "hk_aeronave"],
    "Link: relacion entre un vuelo y su aeronave.",
)

# ---------- Plata: satelite del vuelo (SCD2 por hashdiff) ----------
dp.create_streaming_table(name=SAT_VUELO, comment="Satelite: historia de atributos del vuelo; __END_AT nulo = vigente.")
dp.create_auto_cdc_flow(
    target=SAT_VUELO,
    source=STAGING,
    keys=["hk_vuelo"],
    sequence_by="fecha_carga",
    column_list=["hk_vuelo", "hashdiff", *ATRIBUTOS, "fecha_carga", "fuente_registro"],
    stored_as_scd_type=2,
    track_history_column_list=["hashdiff"],
)


# ---------- Oro: vistas materializadas (lectura batch sobre Plata) ----------
@dp.materialized_view(name=POR_PAIS, comment="Oro: eventos validos y velocidad promedio por pais.")
def r03_vuelos_por_pais():
    return (
        spark.read.table(SAT_VUELO)
        .where("__END_AT IS NULL")
        .groupBy("pais_origen")
        .agg(F.count("*").alias("eventos"), F.avg("velocidad_kmh").alias("velocidad_promedio"))
    )


@dp.materialized_view(name=POR_AERONAVE, comment="Oro: eventos validos y velocidad promedio por aeronave (link + hub + satelite).")
def r03_vuelos_por_aeronave():
    link = spark.read.table(LINK_VUELO_AERONAVE).select("hk_vuelo", "hk_aeronave")
    aeronaves = spark.read.table(HUB_AERONAVE).select("hk_aeronave", "aeronave_id")
    vigentes = spark.read.table(SAT_VUELO).where("__END_AT IS NULL").select("hk_vuelo", "velocidad_kmh")
    return (
        link.join(aeronaves, "hk_aeronave")
        .join(vigentes, "hk_vuelo")
        .groupBy("aeronave_id")
        .agg(F.count("*").alias("eventos"), F.avg("velocidad_kmh").alias("velocidad_promedio"))
    )
