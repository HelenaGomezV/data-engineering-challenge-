# Infraestructura – MIGx Technical Assignment (AWS + Databricks + PySpark + Evidently)

## 1. Despliegue de la solución en un entorno cloud (AWS + Databricks) no propongo la solucion en azure por que no conozco las tecnologias

### Recursos:

- **Amazon S3**: Almacenamiento de datos crudos, transformados y finales.
- **AWS Databricks**: Plataforma de procesamiento distribuido usando PySpark.
- **AWS MWAA (Managed Workflows for Apache Airflow)**: Orquestación de pipelines.
- **Amazon RDS / Amazon Redshift**: Bases de datos para análisis SQL y reportes.
- **Amazon QuickSight**: Visualización de resultados y dashboards.
- **AWS Secrets Manager**: Gestión segura de credenciales y conexiones.
- **Amazon CloudWatch**: Logs y métricas de rendimiento.

## 2. Manejo de Cambios de Esquema en el Pipeline

### Estrategias:

---

### Validación de esquema con PySpark

Usar `StructType` y `StructField` para definir explícitamente el esquema de entrada:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("age", IntegerType(), True),
    # ...
])

df = spark.read.csv("s3://bucket/data.csv", schema=schema, header=True)
```
### Validación con Evidently

Detecta automáticamente:

* Columnas faltantes
* Cambios en la distribución (data drift)
* Aumento de valores nulos

``` python
from evidently.report import Report
from evidently.metric_preset import DataQualityPreset

report = Report(metrics=[DataQualityPreset()])
report.run(reference_data=ref_df, current_data=current_df)
report.save_html("/dbfs/tmp/evidently_quality_report.html")
```
### Control de versiones

Mantener los esquemas esperados (JSON, PySpark) en Git.

Compararlos en cada ejecución antes de procesar los datos.

###  CI/CD y Testing

Integrar pruebas con pytest y validaciones de esquema.

Bloquear despliegues si los esquemas cambian sin validación.

3. ¿Qué hacer si el pipeline genera datos incorrectos en producción?

Plan de mitigación y recuperación:

###  Monitoreo y alertas

Evidently: Detecta cambios estadísticos anómalos.

Airflow: Alertas si baja la cantidad de registros, aumenta el tiempo de ejecución o falla un paso.

###  Rollback y versionado

Archivos versionados en S3, por ejemplo:

```bash s3://bucket/output/cleaned_data_xxxxx.csv```

Permite restaurar ejecuciones previas si ocurre un fallo.

###  Auditoría y logging

Logs de Airflow, Evidently y Databricks almacenados en CloudWatch o S3.

Guardar metadatos por ejecución:

* Fecha
* Paso ejecutado
* Número de registros
* Reporte de calidad

###  Reprocesamiento modular

Dividir el DAG en pasos independientes:

* Ingestión
* Limpieza
* Validación
* KPIs

Relanzar solo el paso afectado si hay un fallo.

###  Validación previa al guardado

Si Evidently detecta problemas:

* Se bloquea la escritura del archivo final
* Se permite reintento manual o automático
