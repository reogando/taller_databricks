# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. Generar los esquemas en formato medallón (bronce, plata y oro) para comenzar a guardar la data proveniente del Storage Account de Azure (Gen2). 💼🧳💪
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronce;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Guardar la Shared Access Signature (SAS) como secreto usando Databricks Secrets para una conexión segura al Storage. 🔑🔐

# COMMAND ----------

# Verificamos que los secretos existen previamente.

display(dbutils.secrets.listScopes())
display(dbutils.secrets.list("databricks-course-secret-scope"))

# COMMAND ----------

dbc_sas_token = dbutils.secrets.get(scope="databricks-course-secret-scope", key="databricks-course-sas-token")
print(dbc_sas_token)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sadatabrickscourse001.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.sadatabrickscourse001.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.sadatabrickscourse001.dfs.core.windows.net", dbc_sas_token)

# COMMAND ----------

#Comprobamos que tenemos acceso
display(dbutils.fs.ls("abfss://container-course-databricks-001@sadatabrickscourse001.dfs.core.windows.net"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://container-course-databricks-001@sadatabrickscourse001.dfs.core.windows.net/clientes/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Realizar la ingesta de los archivos CSV usando Auto Loader: ☁️☁️
# MAGIC - Utilizar el formato 'cloudFiles'.
# MAGIC - Habilitar la evolución del esquema (schema evolution) en lectura y escritura.
# MAGIC - Guardar los datos en formato Delta dentro de un esquema llamado 'bronce'.
# MAGIC - Utilizamos metadata.
# MAGIC - Agregamos columnas para auditoría.
# MAGIC
# MAGIC 🚨🚨🚨 IMPORTANTE 🚨🚨🚨
# MAGIC
# MAGIC Utilizamos "awaitTermination" para asegurarnos que todo se cargue correctamente. 
# MAGIC

# COMMAND ----------

# Cargamos las librerías necesarias

from pyspark.sql.functions import col, schema_of_json, from_json, lit, when, current_timestamp, input_file_name, expr, max, row_number, desc
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StringType, NumericType
from delta.tables import DeltaTable
import json, uuid, time


# COMMAND ----------

# Guardamos los nombres del storage y del contenedor desde Azure Storage Gen2.
container = "container-course-databricks-001"
storage = "sadatabrickscourse001"

# Generamos la función para ingestar los datos.
def ingestar_auto_loader(location, table_name):
  path_location_azure = f"abfss://{container}@{storage}.dfs.core.windows.net/{location}"
  schema_location = f"/Filestore/course/databricks/advanced/autoloader/schema/datapath/data/{table_name}"
  checkpoint_location = f"/Filestore/course/databricks/advanced/autoloader/checkpoint/datapath/dataclass/{table_name}"

# Leemos los datos en modo streaming con rescue.
  df_streaming = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(path_location_azure)
  )

# Enriquecemos con metadata
  df_enriched = (df_streaming
    .withColumn("ingestion_ts", current_timestamp()) # Marca temporal de ingesta
    .withColumn("ingestion_id", expr("uuid()")) # ID único de ingesta
    .withColumn("source_file", col("_metadata.file_name")) # Nombre del archivo
    .withColumn("source_path", col("_metadata.file_path")) # Ruta completa
    .withColumn("file_modification_time", col("_metadata.file_modification_time")) # Modificación del archivo en el storage
    .withColumn("file_size", col("_metadata.file_size")) # Tamaño del archivo
  )

# Guardamos en bronze
  (df_enriched 
      .writeStream
      .trigger(availableNow=True)
      .option("mergeSchema", "true")
      .option("checkpointLocation", checkpoint_location)
      .toTable(f"hive_metastore.bronce.{table_name}")
      .awaitTermination()
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Limpiar duplicados y valores nulos, y mover los datos al esquema 'plata'. 🛠️⚡
# MAGIC - Detectamos cambios con la columna rescue_data.
# MAGIC - Inferimos esquema.
# MAGIC - Utilizamos funciones para simplificar la lectura.
# MAGIC - Guardamos Delta.

# COMMAND ----------

def replace_nulls(df):

    # Detectamos columnas string y numéricas.
    str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    num_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]

    # Reemplazamos nulos por "NA" o 0 según tipo de dato
    return df.fillna("NA", subset=str_cols).fillna(0, subset=num_cols)
    

# COMMAND ----------

location = dbutils.widgets.get("location")
table_name = dbutils.widgets.get("table_name")

def limpiar_y_guardar_silver(table_name):

    # Cargamos la tabla desde bronce.
    df_bronce = spark.table(f"hive_metastore.bronce.{table_name}")

    # Eliminamos duplicados y nulos.
    id_col = df_bronce.columns[0]
    df_bronce = df_bronce.filter(col(id_col).isNotNull())

    window = Window.partitionBy(id_col).orderBy(desc("ingestion_ts"))

    df_bronce = (
        df_bronce
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    df_bronce = replace_nulls(df_bronce)
    
    has_rescued_data = "_rescued_data" in df_bronce.columns
    
    # Buscamos rescues del último batch, no de todo el histórico.
    if has_rescued_data:

        last_ts = df_bronce.agg(max("ingestion_ts")).collect()[0][0]

        # Filtramos solo ese batch y con rescued_data válido.
        df_rescued = (df_bronce
            .filter(col("ingestion_ts") == last_ts)
            .filter((col("_rescued_data").isNotNull()) & (col("_rescued_data") != "NA"))
        )

        rescued_non_empty = df_rescued.limit(1).count() > 0 
    else:
        rescued_non_empty = False

    # Si la columna "_rescued_data" contiene datos, intentamos parsearla.
    if has_rescued_data and rescued_non_empty:

        # Recorremos los rescue
        rescued_rows = df_rescued.limit(50).collect()

        for row in rescued_rows:
            rescued_json_str = row["_rescued_data"]

            # Inferimos el schema a partir de cada JSON nuevo.
            schema_col = schema_of_json(lit(rescued_json_str))
            schema_str = spark.range(1).select(schema_col.alias("schema_json")).collect()[0]["schema_json"]
            inferred_schema = StructType.fromDDL(schema_str)

            # Parseamos y extraemos campos nuevos.
            df_bronce = df_bronce.withColumn("rescued_json", from_json(col("_rescued_data"), inferred_schema))
            for field in inferred_schema.fieldNames():
                if field not in df_bronce.columns:
                    df_bronce = df_bronce.withColumn(field, col("rescued_json").getField(field))

        # Limpiamos columnas auxiliares.
        cols_to_drop = [c for c in ["_rescued_data", "rescued_json", "_file_path"] if c in df_bronce.columns]
        df_silver = df_bronce.drop(*cols_to_drop).dropDuplicates()

    else:
        
        df_silver = df_bronce.drop("_rescued_data").dropDuplicates()

    df_silver = replace_nulls(df_silver)

    # Ruta y nombre de la tabla destino.
    path_silver = f"dbfs:/user/hive/warehouse/silver.db/{table_name}"
    full_table_name = f"hive_metastore.silver.{table_name}"

    # Guardamos como Delta Table (creamos o actualizamos).
    if not DeltaTable.isDeltaTable(spark, path_silver):
        df_silver.write.format("delta").saveAsTable(full_table_name)
    else:

        # Habilitamos la evolución del esquema.
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        delta_tbl = DeltaTable.forPath(spark, path_silver)
        id_col = df_silver.columns[0]
        delta_tbl.alias("t").merge(df_silver.alias("s"), f"t.{id_col} = s.{id_col}") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

# Ejecutamos ingesta y limpieza.
ingestar_auto_loader(location, table_name)
limpiar_y_guardar_silver(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##5. Crear una tabla final consolidada en el esquema 'oro' que combine: 🔚
# MAGIC - Ventas
# MAGIC - Información de clientes
# MAGIC - Detalles de productos
# MAGIC - Categorías

# COMMAND ----------

# Generamos una función para eliminar columnas de auditoria.

AUDIT_COLS = [
    "ingestion_ts",
    "ingestion_id",
    "source_file",
    "source_path",
    "file_modification_time",
    "file_size"
]

def drop_audit_cols(df):
    cols_to_drop = [c for c in AUDIT_COLS if c in df.columns]
    return df.drop(*cols_to_drop) if cols_to_drop else df


# COMMAND ----------

tables = ["clientes_autoloader", "productos_autoloader", "categorias_autoloader", "ventas_autoloader"]

if all(spark.catalog.tableExists(f"hive_metastore.silver.{t}") for t in tables):

    clientes = spark.table("hive_metastore.silver.clientes_autoloader")
    productos = spark.table("hive_metastore.silver.productos_autoloader")
    categorias = spark.table("hive_metastore.silver.categorias_autoloader")
    ventas = spark.table("hive_metastore.silver.ventas_autoloader")

    # Realizamos los joins entre hechos y dimensiones eliminando los campos de auditoria.
    df_gold = drop_audit_cols(ventas) \
        .join(drop_audit_cols(clientes), on="cliente_id", how="inner") \
        .join(drop_audit_cols(productos), on="producto_id", how="inner") \
        .join(drop_audit_cols(categorias), on="categoria_id", how="inner") \

    df_gold = df_gold.dropDuplicates()

    gold_table_name = "hive_metastore.gold.tabla_final_consolidada"
    gold_path_name = "dbfs:/user/hive/warehouse/gold.db/tabla_final_consolidada"

    if not DeltaTable.isDeltaTable(spark, gold_path_name):
        df_gold.write.format("delta").saveAsTable(gold_table_name)
        print("Tabla oro creada correctamente.")
    else:
        delta_oro = DeltaTable.forPath(spark, gold_path_name)
        delta_oro.alias("t").merge(df_gold.alias("s"), "t.venta_id = s.venta_id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

        print("Tabla oro actualizada con nuevos datos o cambios.")

else:
    print("No están disponibles todas las tablas necesarias.")