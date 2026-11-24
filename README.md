# Reto 3: arquitectura medallón para indicadores de vuelos

**Nivel:** intermedio · **Duración sugerida:** 3–4 horas  
**Modalidad:** individual o en parejas.

## Objetivo

Construir un flujo declarativo Bronze → Silver → Gold a partir de datos sintéticos. Bronze conserva los eventos; Silver aplica una regla de calidad; Gold presenta indicadores por país.

## Resultado esperado

El archivo de entrada contiene cuatro eventos. Bronze conserva los 4; Silver acepta 3 y descarta 1 con velocidad negativa. Gold contiene:

| País | Eventos válidos | Velocidad promedio |
|---|---:|---:|
| CR | 2 | 225 |
| PA | 1 | 300 |

El grafo debe mostrar `vuelos_bronze → vuelos_silver → vuelos_gold`.

## Requisitos

- Workspace de Databricks con Unity Catalog, Lakeflow pipelines, expectations, Auto Loader y cómputo compatible.
- Catálogo y esquema propios o asignados; permisos para usarlos, crear streaming tables, una vista materializada y un volumen, leer/escribir en el volumen y ejecutar el pipeline.
- Acceso a SQL Editor o notebook. El cómputo y almacenamiento pueden generar costos.
- Conocer streaming tables y ejecución incremental; se recomienda haber completado los retos 1 y 2.

Utiliza un esquema y una carpeta de entrada independientes por participante y por reto: los tres pipelines reutilizan algunos nombres de tabla. Sustituye `CATALOGO` y `ESQUEMA` por tus valores reales.

## Paso a paso

1. **Prepara el volumen.** En SQL Editor o notebook ejecuta:

   ```sql
   CREATE VOLUME IF NOT EXISTS CATALOGO.ESQUEMA.retos_sdp;
   ```

   Crea la carpeta vacía `reto03/entrada` en el volumen mediante Catalog Explorer y sube `datos/lote_01.json`.

2. **Diseña las capas.** Define Bronze como streaming table de entrada; Silver como streaming table que lee Bronze incrementalmente y descarta velocidades nulas o negativas; Gold como vista materializada agrupada por país.

3. **Crea el código y pipeline.** Intenta implementar el diseño y compara con `solucion/pipeline.py`. Crea el archivo Python en el workspace y agrégalo a un nuevo pipeline. Configura catálogo y esquema destino, modo **triggered**, y en **Settings → Configuration** la clave `lab.source_path` con `/Volumes/CATALOGO/ESQUEMA/retos_sdp/reto03/entrada`.

4. **Ejecuta.** Inicia una actualización normal y espera el resultado. Revisa el grafo y las métricas de calidad de `vuelos_silver`: la expectativa `velocidad_valida` debe registrar un evento inválido descartado.

5. **Valida.** Ejecuta las consultas de abajo. Compara los resultados de Gold tanto con la tabla esperada como con el cálculo independiente sobre Silver.

6. **Justifica las decisiones.** Explica por qué Bronze conserva el dato inválido, qué implica descartarlo de Silver y por qué Gold utiliza lectura batch sobre Silver.

## Validación SQL

```sql
SELECT COUNT(*) AS eventos_bronze FROM CATALOGO.ESQUEMA.vuelos_bronze;
SELECT COUNT(*) AS eventos_silver FROM CATALOGO.ESQUEMA.vuelos_silver;

SELECT pais_origen, eventos, velocidad_promedio
FROM CATALOGO.ESQUEMA.vuelos_gold ORDER BY pais_origen;

SELECT pais_origen, COUNT(*) AS eventos, AVG(velocidad_kmh) AS velocidad_promedio
FROM CATALOGO.ESQUEMA.vuelos_silver
GROUP BY pais_origen ORDER BY pais_origen;
```

## Entregables y evaluación

- Grafo y resultados de las cuatro consultas.
- Evidencia de la expectativa y explicación de las tres capas.
- Aprobación: Bronze = 4, Silver = 3, un descarte y Gold coincide con los valores esperados y con el cálculo independiente.
- Reflexión: ¿cómo conservarías los registros rechazados para revisión en vez de descartarlos?

## Referencias

- [Conceptos de pipelines](https://docs.databricks.com/aws/en/ldp/concepts)
- [Desarrollo Python con expectations](https://docs.databricks.com/aws/en/ldp/developer/python-dev)
- [Parámetros de configuración](https://docs.databricks.com/aws/en/ldp/parameters)

Las streaming tables y las vistas materializadas son objetos administrados por Unity Catalog. La vista materializada puede actualizarse incrementalmente cuando es compatible o requerir recálculo.
