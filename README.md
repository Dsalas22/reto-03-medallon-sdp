# Reto 3: arquitectura medallón para indicadores de vuelos

**Nivel:** intermedio · **Duración sugerida:** 3–4 horas  
**Modalidad:** individual o en parejas.

## Objetivo

Construir un flujo declarativo medallón completo a partir de datos sintéticos, con capas fijas: **Bronce** (`raw_data.bronze`) conserva todos los eventos; un staging privado aplica una regla de calidad; **Plata** (`data_vault.silver`) modela los datos válidos como **Data Vault** con dos hubs, un link y un satélite; **Oro** (`ods.gold`) publica indicadores por país y por aeronave en vistas materializadas.

## Resultado esperado

El archivo de entrada contiene cuatro eventos. Bronce conserva los 4; la expectativa `velocidad_valida` descarta 1 con velocidad negativa (`e4`), así que Plata recibe 3:

| Tabla | Filas |
|---|---:|
| `raw_data.bronze.r03_vuelos_raw_dsh` | 4 |
| `data_vault.silver.r03_hub_vuelo_dsh` | 3 |
| `data_vault.silver.r03_hub_aeronave_dsh` | 2 (A y B) |
| `data_vault.silver.r03_link_vuelo_aeronave_dsh` | 3 |
| `data_vault.silver.r03_sat_vuelo_dsh` | 3 |

Oro:

| `r03_vuelos_por_pais_dsh` | Eventos válidos | Velocidad promedio |
|---|---:|---:|
| CR | 2 | 225 |
| PA | 1 | 300 |

| `r03_vuelos_por_aeronave_dsh` | Eventos válidos | Velocidad promedio |
|---|---:|---:|
| A | 2 | 225 |
| B | 1 | 300 |

## Arquitectura y tablas

| Capa | Objeto (sufijo `dsh`) | Tipo | Contenido |
|---|---|---|---|
| Aterrizaje | `/Volumes/raw_data/bronze/landing_retos_sdp/dsh/reto03/entrada` | Carpeta en volumen compartido | Archivos JSON del generador |
| Bronce | `raw_data.bronze.r03_vuelos_raw_dsh` | Streaming table (Auto Loader) | Todo lo que llega, incluso inválidos, + `archivo_origen`, `fecha_archivo`, `fecha_ingesta` |
| Staging | `r03_stg_vuelos_dsh` | Vista temporal privada con expectativa | Descarta velocidad nula o negativa; calcula hash keys, hashdiff, `fecha_carga`, `fuente_registro` |
| Plata | `data_vault.silver.r03_hub_vuelo_dsh` | Streaming table, AUTO CDC SCD1 | Una fila por `evento_id` (primera carga) |
| Plata | `data_vault.silver.r03_hub_aeronave_dsh` | Streaming table, AUTO CDC SCD1 | Una fila por `aeronave_id` (primera carga) |
| Plata | `data_vault.silver.r03_link_vuelo_aeronave_dsh` | Streaming table, AUTO CDC SCD1 | Relación vuelo–aeronave (primera carga) |
| Plata | `data_vault.silver.r03_sat_vuelo_dsh` | Streaming table, AUTO CDC SCD2 | País, velocidad y persona del vuelo; nueva versión solo si cambia el hashdiff |
| Oro | `ods.gold.r03_vuelos_por_pais_dsh` | Vista materializada | Eventos y velocidad promedio por país (satélite vigente) |
| Oro | `ods.gold.r03_vuelos_por_aeronave_dsh` | Vista materializada | Link + hub de aeronave + satélite vigente |

## Data Vault en pocas palabras

- **Hub:** lista única de una llave de negocio. Aquí hay dos: vuelos (`evento_id`) y aeronaves (`aeronave_id`). Cada uno tiene su hash key (`hk_vuelo`, `hk_aeronave`) = SHA-256 de la llave normalizada (sin espacios y en mayúsculas).
- **Link:** registra que dos llaves están relacionadas: “el vuelo e1 lo hizo la aeronave A”. Su hash key `hk_vuelo_aeronave` es el SHA-256 de ambas llaves normalizadas unidas con `||`. Si mañana un vuelo cambiara de aeronave, se agregaría otro link sin tocar los hubs.
- **Satélite:** atributos descriptivos que cambian en el tiempo (país, velocidad, persona), colgados del hub de vuelos. El **hashdiff** detecta cambios con una sola comparación; SCD2 guarda la historia con `__START_AT` y `__END_AT`.
- **`fecha_carga` y `fuente_registro`** permiten auditar cuándo y de qué archivo llegó cada fila.
- **Oro** reconstruye la vista de negocio uniendo link + hub + satélite vigente (`__END_AT IS NULL`).

## Requisitos

- Workspace de Databricks con Unity Catalog, Lakeflow pipelines, expectations, Auto Loader y cómputo compatible.
- No creas catálogos, esquemas ni volúmenes: el instructor ya preparó las capas fijas, el volumen compartido y los permisos (ver la sección siguiente).
- Acceso a SQL Editor o notebook. El cómputo y almacenamiento pueden generar costos.
- Permisos para crear y ejecutar Lakeflow Jobs y escribir en el volumen con la identidad que ejecuta el Job.
- Conocer streaming tables y ejecución incremental; se recomienda haber completado los retos 1 y 2.

## Preparación del instructor (una vez)

Los catálogos `raw_data`, `data_vault` y `ods` y los esquemas `raw_data.bronze`, `data_vault.silver` y `ods.gold` ya existen. El instructor crea el volumen compartido y otorga los permisos una sola vez; sirven para los tres retos. Sustituye `participantes` por el grupo de participantes (recomendado) o por un correo:

```sql
CREATE VOLUME IF NOT EXISTS raw_data.bronze.landing_retos_sdp;

GRANT USE CATALOG ON CATALOG raw_data   TO `participantes`;
GRANT USE CATALOG ON CATALOG data_vault TO `participantes`;
GRANT USE CATALOG ON CATALOG ods        TO `participantes`;

GRANT USE SCHEMA, CREATE TABLE, SELECT ON SCHEMA raw_data.bronze  TO `participantes`;
GRANT USE SCHEMA, CREATE TABLE, SELECT ON SCHEMA data_vault.silver TO `participantes`;
GRANT USE SCHEMA, CREATE MATERIALIZED VIEW, SELECT ON SCHEMA ods.gold TO `participantes`;

GRANT READ VOLUME, WRITE VOLUME ON VOLUME raw_data.bronze.landing_retos_sdp TO `participantes`;
```

**Advertencia: el volumen es compartido.** Con `WRITE VOLUME` cualquier participante puede leer, escribir y borrar archivos en cualquier carpeta del volumen, no solo en la suya. Cada persona debe trabajar únicamente en `landing_retos_sdp/<su_sufijo>/` y nunca borrar archivos ajenos. Usa solo datos dummy. Con `SELECT` en los esquemas, los participantes también pueden consultar las tablas de los demás; cada quien es dueño solo de las tablas que crea su pipeline. Los participantes necesitan además permiso para crear pipelines (serverless o con una política de clúster) y Jobs.

## Identificación por participante

Forma un sufijo con la primera letra del nombre y de cada uno de los dos apellidos, en minúsculas y sin tildes; por ejemplo, `dsh`. Usa tres letras ASCII (`a`–`z`). Si dos participantes tienen las mismas iniciales o una persona no puede formar tres letras, el instructor asignará a cada uno un sufijo alternativo de tres letras **único**. Confírmalo antes de crear recursos.

Todos los ejemplos usan `dsh`: sustitúyelo por tu sufijo en **cada** nombre, consulta y ruta. Todas las tablas llevan el prefijo del reto (`r03_`) y tu sufijo, por ejemplo `raw_data.bronze.r03_vuelos_raw_dsh`, para no chocar con otros retos ni con otras personas. Tu carpeta de aterrizaje es `/Volumes/raw_data/bronze/landing_retos_sdp/dsh/reto03/entrada`; el pipeline la deriva del sufijo. La separación por nombres no reemplaza permisos de Unity Catalog ni protege frente al acceso de otros usuarios.

Los pipelines existentes con nombres anteriores no se migran automáticamente: crea un pipeline nuevo con sus recursos propios; no cambies el sufijo en un pipeline compartido o ya ejecutado.

## Datos ficticios y Job

Los campos `persona_id` y `persona_alias` son identificadores y etiquetas inventados, como `DUMMY-ID-001` y `DUMMY_UCRANIA`; los nombres de países son solo alias, no representan personas reales. No se usan documentos, correos, direcciones ni otros datos personales reales. No cargues información real en este laboratorio.

1. Importa `ingesta/generador.py` como notebook de Databricks o selecciónalo desde una Git folder.
2. En **Jobs & Pipelines**, crea el Job `job_ingesta_reto03_dsh` con una tarea **Notebook** que apunte al generador y use cómputo compatible.
3. En los **parámetros del Job** configura `sufijo = dsh` y `lote = lote_01`, sustituyendo `dsh` por tu sufijo. No dupliques estos parámetros en la tarea. Guarda el Job sin programación, con concurrencia máxima de una ejecución.
4. Cuando el instructor confirme la preparación, ejecuta **Run now** y espera a que termine correctamente. Si el volumen no existe o no tienes permiso, el Job falla con un mensaje que lo indica. Genera cuatro eventos, uno con velocidad negativa para probar calidad. El Job no sobrescribe el archivo: no ejecutes `lote_01` dos veces en la misma carpeta.

Los archivos de `datos/` son muestras de referencia equivalentes; no los subas si ya usas el Job, para no duplicar la ingesta.

## Paso a paso

1. **Genera el lote.** Ejecuta el Job manual descrito arriba con `lote = lote_01`. La salida debe decir `Lote lote_01: 4 eventos escritos en /Volumes/raw_data/bronze/landing_retos_sdp/dsh/reto03/entrada/lote_01.json`.

2. **Diseña las capas.** Bronce: streaming table de entrada con columnas técnicas. Staging: vista temporal que lee Bronce incrementalmente, descarta velocidades nulas o negativas y calcula hash keys y hashdiff. Plata: dos hubs, un link y un satélite. Oro: dos vistas materializadas.

3. **Crea el código y pipeline.** Intenta implementar el diseño y compara con `solucion/pipeline.py`. Crea el archivo Python en el workspace y agrégalo a un nuevo pipeline llamado `pipeline_reto03_dsh`. Destino por defecto: catálogo `raw_data`, esquema `bronze`; modo **triggered**. Plata y Oro se publican en `data_vault.silver` y `ods.gold` porque el código usa el nombre completo de cada tabla. En **Settings → Configuration** agrega solo `pipelines.suffix = dsh`; el código rechaza un sufijo ausente o que no tenga exactamente tres letras. Las claves opcionales `raw_catalog`, `raw_schema`, `silver_catalog`, `silver_schema`, `gold_catalog`, `gold_schema` y `landing_volume` ya tienen como valores por defecto `raw_data`, `bronze`, `data_vault`, `silver`, `ods`, `gold` y `landing_retos_sdp`.

4. **Ejecuta.** Inicia una actualización normal y espera el resultado. Revisa el grafo y las métricas de calidad: la expectativa `velocidad_valida` del staging `r03_stg_vuelos_dsh` debe registrar 3 aceptados y 1 descartado. Como el staging es una vista temporal, la métrica aparece en cada flujo que lo lee (hubs, link y satélite) dentro de los eventos `flow_progress` del event log; usa la consulta de la sección de validación.

5. **Valida.** Ejecuta las consultas de abajo. Compara Oro con las tablas esperadas y con el cálculo independiente sobre Plata.

6. **Justifica las decisiones.** Explica por qué Bronce conserva el dato inválido, qué implica descartarlo antes de Plata, por qué la aeronave es un hub y no un atributo, y por qué Oro usa lectura batch sobre Plata.

## Validación SQL

```sql
-- Conteos por capa
SELECT
  (SELECT COUNT(*) FROM raw_data.bronze.r03_vuelos_raw_dsh)              AS bronce,
  (SELECT COUNT(*) FROM data_vault.silver.r03_hub_vuelo_dsh)              AS hub_vuelo,
  (SELECT COUNT(*) FROM data_vault.silver.r03_hub_aeronave_dsh)           AS hub_aeronave,
  (SELECT COUNT(*) FROM data_vault.silver.r03_link_vuelo_aeronave_dsh)    AS link,
  (SELECT COUNT(*) FROM data_vault.silver.r03_sat_vuelo_dsh)              AS sat;

-- El evento inválido está en Bronce pero no en Plata
SELECT b.evento_id, b.velocidad_kmh, h.evento_id IS NOT NULL AS en_plata
FROM raw_data.bronze.r03_vuelos_raw_dsh b
LEFT JOIN data_vault.silver.r03_hub_vuelo_dsh h USING (evento_id)
ORDER BY b.evento_id;

-- Vista de negocio reconstruida: link + hubs + satélite vigente
SELECT hv.evento_id, ha.aeronave_id, s.pais_origen, s.velocidad_kmh, s.persona_alias
FROM data_vault.silver.r03_link_vuelo_aeronave_dsh l
JOIN data_vault.silver.r03_hub_vuelo_dsh hv    USING (hk_vuelo)
JOIN data_vault.silver.r03_hub_aeronave_dsh ha USING (hk_aeronave)
JOIN data_vault.silver.r03_sat_vuelo_dsh s     USING (hk_vuelo)
WHERE s.__END_AT IS NULL
ORDER BY hv.evento_id;

-- Oro
SELECT * FROM ods.gold.r03_vuelos_por_pais_dsh     ORDER BY pais_origen;
SELECT * FROM ods.gold.r03_vuelos_por_aeronave_dsh ORDER BY aeronave_id;

-- Cálculo independiente por país sobre el satélite vigente
SELECT pais_origen, COUNT(*) AS eventos, AVG(velocidad_kmh) AS velocidad_promedio
FROM data_vault.silver.r03_sat_vuelo_dsh
WHERE __END_AT IS NULL
GROUP BY pais_origen ORDER BY pais_origen;

-- Métricas de la expectativa. Sustituye <pipeline_id> por el ID del pipeline
-- (visible en la página del pipeline); requiere ser propietario del pipeline.
SELECT timestamp, origin.flow_name,
       details:flow_progress.data_quality.expectations AS expectativas
FROM event_log('<pipeline_id>')
WHERE event_type = 'flow_progress'
  AND origin.flow_name LIKE '%r03_sat_vuelo_dsh'
  AND details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp;
```

Resultados esperados: conteos 4 / 3 / 2 / 3 / 3; `e4` (velocidad -20) aparece en Bronce con `en_plata = false`; la vista de negocio lista `e1`–`e3`; Oro coincide con las tablas esperadas y con el cálculo independiente; la expectativa muestra `"name":"velocidad_valida"`, `"dataset":"r03_stg_vuelos_dsh"`, `passed_records = 3` y `failed_records = 1`.

## Ingesta sintética cada 3 minutos

Después de completar las comprobaciones manuales:

1. Crea otro Job `job_programado_reto03_dsh` con una tarea **Notebook** llamada `generar_datos` que use `ingesta/generador.py`.
2. En los **parámetros del Job**, configura `sufijo = dsh`, `lote = continuo` y `registros = 500`. Sustituye el sufijo; no dupliques parámetros en la tarea. `registros` solo aplica a `continuo`, acepta enteros de **1 a 500** y vale 500 por defecto. Un valor inválido hace fallar la tarea antes de escribir.
3. Añade una tarea **Pipeline** llamada `transformar_datos`, dependiente del **éxito** de `generar_datos`, y selecciona `pipeline_reto03_dsh`. Usa actualización normal, **no full refresh**.
4. En **Add trigger → Scheduled → Advanced → Show Cron Syntax**, configura la expresión Quartz `0 0/3 * * * ?` y zona horaria `UTC`: minutos 00, 03, 06, etc. Guarda la programación inicialmente **pausada**.
5. Configura concurrencia máxima **1**, desactiva **Queue** y deja los reintentos automáticos de la tarea generadora en **0**. Si una ejecución dura más de 3 minutos, las siguientes pueden omitirse mientras sigue activa; no se superponen ni se acumulan. No se garantiza una ingesta exactamente cada 3 minutos.
6. Ejecuta **Run now** para verificar permisos y la cadena Notebook → Pipeline. Después activa la programación, observa algunas ejecuciones y vuelve a **pausarla** al terminar.

Cada ejecución exitosa del generador escribe **un archivo JSON Lines nuevo con 500 eventos** (o la cantidad elegida), sin sobrescribir. Los identificadores de evento, persona y archivo se generan con UUID e índice. Las aeronaves, países y velocidades varían usando solo valores dummy; no hay conexión a fuentes reales. El lote fijo `lote_01` no cambia.

En el modo continuo, **cada décimo evento tiene velocidad negativa**: se generan `floor(registros / 10)` inválidos para observar la expectativa en cada lote de 10 o más registros. Con 500, Bronce recibe **500**, la expectativa acepta **450** y descarta **50**. Con menos de 10, todos son válidos. Tras la prueba inicial y un lote de 500 procesado correctamente, los acumulados deben ser Bronce = **504** y hub de vuelos, link y satélite = **453** cada uno; el hub de aeronaves tiene como máximo **5** (A–E). Verifica las métricas de calidad y compara Oro con la agregación independiente de la sección SQL; sus promedios varían y los valores iniciales ya no aplican.

El límite de 500 es **por ejecución del generador**, no para el total acumulado en Delta. Los reintentos manuales pueden producir otro lote; no se garantiza deduplicación de negocio. Esta programación aproxima near real-time, sin garantía de latencia: el planificador y el cómputo pueden retrasar ejecuciones y generan costos. Este repositorio no crea ni activa Jobs automáticamente; configura y valida la programación en tu workspace.

## Entregables y evaluación

- Grafo y resultados de las consultas de validación.
- Evidencia de la expectativa y explicación de las capas y de hub, link y satélite.
- Aprobación: Bronce = 4, hub de vuelos = 3, hub de aeronaves = 2, link = 3, satélite = 3, un descarte, y Oro coincide con los valores esperados y con el cálculo independiente.
- Reflexión: ¿cómo conservarías los registros rechazados para revisión en vez de descartarlos?

## Videos tutoriales en español

Ruta de estudio sugerida; los videos son material de apoyo, no sustituyen el paso a paso. Las duraciones son aproximadas y no están incluidas en la duración del reto. Enlaces y disponibilidad revisados el **25 de noviembre de 2026**; YouTube requiere conexión a Internet.

| Orden y video | Canal / duración | Qué estudiar y cómo aplicarlo |
|---|---|---|
| 1. [Arquitectura Medallion en Databricks: Pipeline ETL completo, Bronze, Silver y Gold con Delta Lake](https://www.youtube.com/watch?v=cyDLRr24mBs) | EDUCATRONICOS ISC · 27 min · español | Responsabilidad de cada capa, schemas, volumes, transformación y agregaciones. Aplica la separación de capas al flujo Bronce → Plata (Data Vault) → Oro del reto; la implementación del video es ETL con notebooks, no la solución SDP. |
| 2. [Lakeflow: Declarative Pipelines & Connect — Workshop Práctico](https://www.youtube.com/watch?v=6ISOHWFoK8w) | Databricks America Latina · 104 min · español | Refuerza el enfoque declarativo y las dependencias del pipeline. Si ya lo viste en el reto 1, úsalo solo como repaso; Connect y CDC no son requisitos de este reto. |

En el video de Medallion puedes ir directamente a [qué es la arquitectura (00:00)](https://www.youtube.com/watch?v=cyDLRr24mBs), [publicación en Silver (18:54)](https://www.youtube.com/watch?v=cyDLRr24mBs&t=1134s) y [creación de Gold (21:42)](https://www.youtube.com/watch?v=cyDLRr24mBs&t=1302s). Los puntos corresponden a los capítulos publicados por el canal.

Para repasar la orquestación periódica, consulta [Lakeflow Jobs Parte 2: Triggers y Scheduled](https://www.youtube.com/watch?v=mXLtzno4tqo), de **EDUCATRONICOS ISC** (14 min, español).

**Compatibilidad y seguridad:** algunos videos usan **Delta Live Tables (DLT)** o separan Bronze, Silver y Gold en tareas Notebook. Aquí las tres capas son datasets de **un pipeline SDP** y el Job solo encadena generación → actualización del pipeline. Usa `from pyspark import pipelines as dp`, `@dp.expect_or_drop` y el código de `solucion/pipeline.py` como referencia de la regla de calidad; para su semántica consulta la documentación oficial de abajo. No copies credenciales ni datasets externos: conserva el generador dummy, tu sufijo y el límite de 500 registros por ejecución.

**Comprobación de estudio:** justifica por qué Bronce conserva los negativos, el staging los descarta antes de Plata y Oro agrega solo los válidos. Comprueba 4/3 en el lote fijo y 500/450 por lote continuo de 500.

## Referencias

- [Conceptos de pipelines](https://learn.microsoft.com/es-es/azure/databricks/ldp/concepts)
- [Desarrollo Python con expectations](https://learn.microsoft.com/es-es/azure/databricks/ldp/developer/python-dev)
- [Parámetros de configuración](https://learn.microsoft.com/es-es/azure/databricks/ldp/parameters)
- [Tareas notebook en Jobs](https://learn.microsoft.com/es-es/azure/databricks/jobs/tasks/notebook)
- [Tareas pipeline en Jobs](https://learn.microsoft.com/es-es/azure/databricks/jobs/tasks/pipeline)
- [Programar Jobs](https://learn.microsoft.com/es-es/azure/databricks/jobs/scheduled)
- [AUTO CDC: SCD tipo 1 y tipo 2](https://learn.microsoft.com/es-es/azure/databricks/ldp/cdc)

Las streaming tables y las vistas materializadas son objetos administrados por Unity Catalog. La vista materializada puede actualizarse incrementalmente cuando es compatible o requerir recálculo.
