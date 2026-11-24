# Databricks notebook source
import json
import re
from uuid import uuid4

dbutils.widgets.text("sufijo", "")
dbutils.widgets.text("lote", "")

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
    token = uuid4().hex
    eventos = [{
        "evento_id": f"dummy_{token}",
        "aeronave_id": "A",
        "pais_origen": "CR",
        "velocidad_kmh": 250,
        "persona_id": f"DUMMY-ID-{token}",
        "persona_alias": "DUMMY_UCRANIA",
    }]
    archivo = f"lote_continuo_{token}.json"

ruta = f"/Volumes/retos_sdp_{sufijo}/reto03_{sufijo}/volumen_sdp_{sufijo}/reto03_{sufijo}/entrada_{sufijo}"
dbutils.fs.mkdirs(ruta)
contenido = "\n".join(json.dumps(evento) for evento in eventos) + "\n"
if not dbutils.fs.put(f"{ruta}/{archivo}", contenido, False):
    raise RuntimeError(f"No se pudo escribir el archivo {archivo}.")
print(f"Lote {lote}: {len(eventos)} eventos escritos en {ruta}/{archivo}")
