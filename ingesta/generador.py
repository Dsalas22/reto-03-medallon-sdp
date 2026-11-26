# Databricks notebook source
# DBTITLE 1,Generador de eventos dummy
import json
import os
import random
import re
from uuid import uuid4

dbutils.widgets.text("sufijo", "")
dbutils.widgets.text("lote", "")
dbutils.widgets.text("registros", "500")
# Opcionales: ubicacion del volumen compartido de aterrizaje (landing).
dbutils.widgets.text("catalogo", "raw_data")
dbutils.widgets.text("esquema", "bronze")
dbutils.widgets.text("volumen", "landing_retos_sdp")

sufijo = dbutils.widgets.get("sufijo").strip().lower()
lote = dbutils.widgets.get("lote").strip().lower()
if not re.fullmatch(r"[a-z]{3}", sufijo):
    raise ValueError("El parametro sufijo debe tener tres letras, por ejemplo dsh.")
if lote not in {"lote_01", "continuo"}:
    raise ValueError("El parametro lote debe ser lote_01 o continuo.")

eventos = [
    {"evento_id": "e1", "aeronave_id": "A", "pais_origen": "CR", "velocidad_kmh": 200, "persona_id": "DUMMY-ID-001", "persona_alias": "DUMMY_UCRANIA"},
    {"evento_id": "e2", "aeronave_id": "B", "pais_origen": "PA", "velocidad_kmh": 300, "persona_id": "DUMMY-ID-002", "persona_alias": "DUMMY_GRECIA"},
    {"evento_id": "e3", "aeronave_id": "A", "pais_origen": "CR", "velocidad_kmh": 250, "persona_id": "DUMMY-ID-003", "persona_alias": "DUMMY_FILIPINAS"},
    {"evento_id": "e4", "aeronave_id": "C", "pais_origen": "GT", "velocidad_kmh": -20, "persona_id": "DUMMY-ID-004", "persona_alias": "DUMMY_UCRANIA"},
]
archivo = "lote_01.json"
if lote == "continuo":
    valor = dbutils.widgets.get("registros").strip()
    if not re.fullmatch(r"[0-9]{1,3}", valor) or not 1 <= int(valor) <= 500:
        raise ValueError("El parametro registros debe ser un entero entre 1 y 500.")
    cantidad = int(valor)
    token = uuid4().hex
    rng = random.Random()
    eventos = []
    for indice in range(cantidad):
        velocidad = float(rng.randint(150, 850))
        if (indice + 1) % 10 == 0:
            velocidad = -float(rng.randint(1, 60))
        eventos.append({
            "evento_id": f"dummy_{token}_{indice:03d}",
            "aeronave_id": rng.choice(["A", "B", "C", "D", "E"]),
            "pais_origen": rng.choice(["CR", "PA", "GT"]),
            "velocidad_kmh": velocidad,
            "persona_id": f"DUMMY-ID-{token}-{indice:03d}",
            "persona_alias": rng.choice(["DUMMY_UCRANIA", "DUMMY_GRECIA", "DUMMY_FILIPINAS"]),
        })
    archivo = f"lote_continuo_{token}.json"

partes = [dbutils.widgets.get(w).strip() for w in ("catalogo", "esquema", "volumen")]
if not all(re.fullmatch(r"[A-Za-z0-9_]+", p) for p in partes):
    raise ValueError("catalogo, esquema y volumen solo admiten letras, numeros y guion bajo.")
volumen = "/Volumes/" + "/".join(partes)
if not os.path.isdir(volumen):
    raise FileNotFoundError(
        f"No existe el volumen {volumen} o no tienes permiso para usarlo. "
        "Pide al instructor que lo cree y te otorgue READ VOLUME y WRITE VOLUME."
    )
ruta = f"{volumen}/{sufijo}/reto03/entrada"
dbutils.fs.mkdirs(ruta)
destino = f"{ruta}/{archivo}"
if os.path.exists(destino):
    raise FileExistsError(f"El archivo {archivo} ya existe en {ruta}. Cada lote fijo se escribe una sola vez: no repitas este lote.")
contenido = "\n".join(json.dumps(evento) for evento in eventos) + "\n"
if not dbutils.fs.put(destino, contenido, False):
    raise RuntimeError(f"No se pudo escribir el archivo {archivo}.")
print(f"Lote {lote}: {len(eventos)} eventos escritos en {ruta}/{archivo}")
