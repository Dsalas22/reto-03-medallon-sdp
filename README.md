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

Para el sufijo de ejemplo `dsh`, el grafo debe mostrar `vuelos_bronze_dsh → vuelos_silver_dsh → vuelos_gold_dsh`.

## Requisitos

- Workspace de Databricks con Unity Catalog, Lakeflow pipelines, expectations, Auto Loader y cómputo compatible.
- Catálogo y esquema propios o asignados; permisos para usarlos, crear streaming tables, una vista materializada y un volumen, leer/escribir en el volumen y ejecutar el pipeline. Crear el catálogo requiere permisos adicionales; puede prepararlo el administrador.
- Acceso a SQL Editor o notebook. El cómputo y almacenamiento pueden generar costos.
- Permisos para crear y ejecutar Lakeflow Jobs y escribir en el volumen con la identidad que ejecuta el Job.
- Conocer streaming tables y ejecución incremental; se recomienda haber completado los retos 1 y 2.

## Identificación por participante

Forma un sufijo con la primera letra del nombre y de cada uno de los dos apellidos, en minúsculas y sin tildes; por ejemplo, `dsh`. Usa tres letras ASCII (`a`–`z`). Si dos participantes tienen las mismas iniciales o una persona no puede formar tres letras, el instructor asignará a cada uno un sufijo alternativo de tres letras **único**. Confírmalo antes de crear recursos.

Todos los ejemplos usan `dsh`: sustitúyelo por tu sufijo en **cada** nombre, consulta y ruta. El catálogo `retos_sdp_dsh` es personal y puede usarse en los tres retos; el esquema `reto03_dsh`, el volumen, la carpeta y el pipeline son específicos de este reto. La separación por nombres no reemplaza permisos de Unity Catalog ni protege frente al acceso de otros usuarios.

Los pipelines existentes con nombres anteriores no se migran automáticamente: crea un pipeline nuevo con sus recursos propios; no cambies el sufijo en un pipeline compartido o ya ejecutado.

## Datos ficticios y Job

Los campos `persona_id` y `persona_alias` son identificadores y etiquetas inventados, como `DUMMY-ID-001` y `DUMMY_UCRANIA`; los nombres de países son solo alias, no representan personas reales. No se usan documentos, correos, direcciones ni otros datos personales reales. No cargues información real en este laboratorio.

1. Importa `ingesta/generador.py` como notebook de Databricks o selecciónalo desde una Git folder.
2. En **Jobs & Pipelines**, crea el Job `job_ingesta_reto03_dsh` con una tarea **Notebook** que apunte al generador y use cómputo compatible.
3. En los **parámetros del Job** configura `sufijo = dsh` y `lote = lote_01`, sustituyendo `dsh` por tu sufijo. No dupliques estos parámetros en la tarea. Guarda el Job sin programación, con concurrencia máxima de una ejecución.
4. Después de crear el volumen del paso 1, ejecuta **Run now** y espera a que termine correctamente. Genera cuatro eventos, uno con velocidad negativa para probar calidad. El Job no sobrescribe el archivo: no ejecutes `lote_01` dos veces en la misma carpeta.

Los archivos de `datos/` son muestras de referencia equivalentes; no los subas si ya usas el Job, para no duplicar la ingesta.

## Paso a paso

1. **Prepara catálogo, esquema y volumen.** El administrador crea o asigna el catálogo personal `retos_sdp_dsh`; verifica con él que no esté asignado a otra persona. Si tienes los permisos necesarios, ejecuta en SQL Editor o notebook:

   ```sql
   CREATE CATALOG IF NOT EXISTS retos_sdp_dsh;
   CREATE SCHEMA IF NOT EXISTS retos_sdp_dsh.reto03_dsh;
   CREATE VOLUME IF NOT EXISTS retos_sdp_dsh.reto03_dsh.volumen_sdp_dsh;
   ```

   Si el catálogo ya fue provisionado, omite la primera sentencia. Usa una carpeta de entrada vacía y ejecuta el Job manual descrito arriba: el notebook crea la carpeta y el primer lote.

2. **Diseña las capas.** Define Bronze como streaming table de entrada; Silver como streaming table que lee Bronze incrementalmente y descarta velocidades nulas o negativas; Gold como vista materializada agrupada por país.

3. **Crea el código y pipeline.** Intenta implementar el diseño y compara con `solucion/pipeline.py`. Crea el archivo Python en el workspace y agrégalo a un nuevo pipeline llamado `pipeline_reto03_dsh`. Configura catálogo `retos_sdp_dsh`, esquema `reto03_dsh`, modo **triggered**, y en **Settings → Configuration** las claves `pipelines.suffix = dsh` y `lab.source_path = /Volumes/retos_sdp_dsh/reto03_dsh/volumen_sdp_dsh/reto03_dsh/entrada_dsh`. El código rechaza un sufijo ausente o que no tenga exactamente tres letras.

4. **Ejecuta.** Inicia una actualización normal y espera el resultado. Revisa el grafo y las métricas de calidad de `vuelos_silver_dsh`: la expectativa `velocidad_valida` debe registrar un evento inválido descartado.

5. **Valida.** Ejecuta las consultas de abajo. Compara los resultados de Gold tanto con la tabla esperada como con el cálculo independiente sobre Silver.

6. **Justifica las decisiones.** Explica por qué Bronze conserva el dato inválido, qué implica descartarlo de Silver y por qué Gold utiliza lectura batch sobre Silver.

## Validación SQL

```sql
SELECT COUNT(*) AS eventos_bronze FROM retos_sdp_dsh.reto03_dsh.vuelos_bronze_dsh;
SELECT COUNT(*) AS eventos_silver FROM retos_sdp_dsh.reto03_dsh.vuelos_silver_dsh;
SELECT evento_id, persona_id, persona_alias
FROM retos_sdp_dsh.reto03_dsh.vuelos_silver_dsh ORDER BY evento_id;

SELECT pais_origen, eventos, velocidad_promedio
FROM retos_sdp_dsh.reto03_dsh.vuelos_gold_dsh ORDER BY pais_origen;

SELECT pais_origen, COUNT(*) AS eventos, AVG(velocidad_kmh) AS velocidad_promedio
FROM retos_sdp_dsh.reto03_dsh.vuelos_silver_dsh
GROUP BY pais_origen ORDER BY pais_origen;
```

## Ingesta sintética cada 3 minutos

Después de completar las comprobaciones manuales:

1. Crea otro Job `job_programado_reto03_dsh` con una tarea **Notebook** llamada `generar_datos` que use `ingesta/generador.py`.
2. En los **parámetros del Job**, configura `sufijo = dsh`, `lote = continuo` y `registros = 500`. Sustituye el sufijo; no dupliques parámetros en la tarea. `registros` solo aplica a `continuo`, acepta enteros de **1 a 500** y vale 500 por defecto. Un valor inválido hace fallar la tarea antes de escribir.
3. Añade una tarea **Pipeline** llamada `transformar_datos`, dependiente del **éxito** de `generar_datos`, y selecciona `pipeline_reto03_dsh`. Usa actualización normal, **no full refresh**.
4. En **Add trigger → Scheduled → Advanced → Show Cron Syntax**, configura la expresión Quartz `0 0/3 * * * ?` y zona horaria `UTC`: minutos 00, 03, 06, etc. Guarda la programación inicialmente **pausada**.
5. Configura concurrencia máxima **1**, desactiva **Queue** y deja los reintentos automáticos de la tarea generadora en **0**. Si una ejecución dura más de 3 minutos, las siguientes pueden omitirse mientras sigue activa; no se superponen ni se acumulan. No se garantiza una ingesta exactamente cada 3 minutos.
6. Ejecuta **Run now** para verificar permisos y la cadena Notebook → Pipeline. Después activa la programación, observa algunas ejecuciones y vuelve a **pausarla** al terminar.

Cada ejecución exitosa del generador escribe **un archivo JSON Lines nuevo con 500 eventos** (o la cantidad elegida), sin sobrescribir. Los identificadores de evento, persona y archivo se generan con UUID e índice. Las aeronaves, países y velocidades varían usando solo valores dummy; no hay conexión a fuentes reales. El lote fijo `lote_01` no cambia.

En el modo continuo, **cada décimo evento tiene velocidad negativa**: se generan `floor(registros / 10)` inválidos para observar la expectativa en cada lote de 10 o más registros. Con 500, Bronze recibe **500**, Silver acepta **450** y descarta **50**. Con menos de 10, todos son válidos. Tras la prueba inicial y un lote de 500 procesado correctamente, los acumulados deben ser Bronze = **504** y Silver = **453**. Verifica las métricas de calidad y compara Gold con la agregación independiente sobre Silver de la sección SQL; sus promedios varían y los valores iniciales ya no aplican.

El límite de 500 es **por ejecución del generador**, no para el total acumulado en Delta. Los reintentos manuales pueden producir otro lote; no se garantiza deduplicación de negocio. Esta programación aproxima near real-time, sin garantía de latencia: el planificador y el cómputo pueden retrasar ejecuciones y generan costos. Este repositorio no crea ni activa Jobs automáticamente; configura y valida la programación en tu workspace.

## Entregables y evaluación

- Grafo y resultados de las cuatro consultas.
- Evidencia de la expectativa y explicación de las tres capas.
- Aprobación: Bronze = 4, Silver = 3, un descarte y Gold coincide con los valores esperados y con el cálculo independiente.
- Reflexión: ¿cómo conservarías los registros rechazados para revisión en vez de descartarlos?

## Videos tutoriales en español

Ruta de estudio sugerida; los videos son material de apoyo, no sustituyen el paso a paso. Las duraciones son aproximadas y no están incluidas en la duración del reto. Enlaces y disponibilidad revisados el **25 de noviembre de 2026**; YouTube requiere conexión a Internet.

| Orden y video | Canal / duración | Qué estudiar y cómo aplicarlo |
|---|---|---|
| 1. [Arquitectura Medallion en Databricks: Pipeline ETL completo, Bronze, Silver y Gold con Delta Lake](https://www.youtube.com/watch?v=cyDLRr24mBs) | EDUCATRONICOS ISC · 27 min · español | Responsabilidad de cada capa, schemas, volumes, transformación y agregaciones. Aplica la separación de capas al flujo Bronze → Silver → Gold del reto; la implementación del video es ETL con notebooks, no la solución SDP. |
| 2. [Lakeflow: Declarative Pipelines & Connect — Workshop Práctico](https://www.youtube.com/watch?v=6ISOHWFoK8w) | Databricks America Latina · 104 min · español | Refuerza el enfoque declarativo y las dependencias del pipeline. Si ya lo viste en el reto 1, úsalo solo como repaso; Connect y CDC no son requisitos de este reto. |

En el video de Medallion puedes ir directamente a [qué es la arquitectura (00:00)](https://www.youtube.com/watch?v=cyDLRr24mBs), [publicación en Silver (18:54)](https://www.youtube.com/watch?v=cyDLRr24mBs&t=1134s) y [creación de Gold (21:42)](https://www.youtube.com/watch?v=cyDLRr24mBs&t=1302s). Los puntos corresponden a los capítulos publicados por el canal.

Para repasar la orquestación periódica, consulta [Lakeflow Jobs Parte 2: Triggers y Scheduled](https://www.youtube.com/watch?v=mXLtzno4tqo), de **EDUCATRONICOS ISC** (14 min, español).

**Compatibilidad y seguridad:** algunos videos usan **Delta Live Tables (DLT)** o separan Bronze, Silver y Gold en tareas Notebook. Aquí las tres capas son datasets de **un pipeline SDP** y el Job solo encadena generación → actualización del pipeline. Usa `from pyspark import pipelines as dp`, `@dp.expect_or_drop` y el código de `solucion/pipeline.py` como referencia de la regla de calidad; para su semántica consulta la documentación oficial de abajo. No copies credenciales ni datasets externos: conserva el generador dummy, tu sufijo y el límite de 500 registros por ejecución.

**Comprobación de estudio:** justifica por qué Bronze conserva los negativos, Silver los descarta y Gold agrega solo los válidos. Comprueba 4/3 en el lote fijo y 500/450 por lote continuo de 500.

## Referencias

- [Conceptos de pipelines](https://docs.databricks.com/aws/en/ldp/concepts)
- [Desarrollo Python con expectations](https://docs.databricks.com/aws/en/ldp/developer/python-dev)
- [Parámetros de configuración](https://docs.databricks.com/aws/en/ldp/parameters)
- [Tareas notebook en Jobs](https://docs.databricks.com/aws/en/jobs/tasks/notebook)
- [Tareas pipeline en Jobs](https://docs.databricks.com/aws/en/jobs/tasks/pipeline)
- [Programar Jobs](https://docs.databricks.com/aws/en/jobs/scheduled)

Las streaming tables y las vistas materializadas son objetos administrados por Unity Catalog. La vista materializada puede actualizarse incrementalmente cuando es compatible o requerir recálculo.
