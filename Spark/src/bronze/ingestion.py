from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.types import *
from pyspark.sql.functions import col

builder = (SparkSession.builder
            .appName("test")
            .enableHiveSupport()
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            )

spark = configure_spark_with_delta_pip(builder).getOrCreate()

catalog = 'bronze'
database = 'postgres'
table = 'financeiro.clientes'
file_format = 'parquet'
id_field = 'cliente_id'
timestamp_field = 'modificado_em'
schema = StructType([
    StructField("cliente_id", LongType(), True),
    StructField("nome", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("modificado_em", TimestampType(), True)
])

table_aux = table.replace(".", "_")
table_formatted = f"{database}_{table_aux}"
table_name = f"{catalog}.{table_formatted}"

def convert_data_types(df,schema):
    schema_dict = {field.name: field.dataType for field in schema.fields}
    for column_name, data_type in schema_dict.items():
        if column_name in df.columns:
            df = df.withColumn(column_name, col(column_name).cast(data_type))
    return df

def table_exists(catalog,table):
    df = spark.sql(f"show tables from {catalog}")
    df = df.filter(df.tableName == table)
    df = df.count()
    return df > 0

def ingest_full_load():
    print(f"creating table {table_name}")
    raw = f's3a://raw/{database}/full_load/{table}/'
    df = (spark.read
        .format(file_format)
        .load(raw)
        )
    df = convert_data_types(df,schema)
    
    (df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(table_name))

def ingest_cdc():
  print(f"ingesting {table_formatted} in cdc")
  raw = f's3a://raw/{database}/cdc/{table}/'
  df_cdc = (spark.read
                .format(file_format)
                .load(raw)
        )
  df_cdc = convert_data_types(df_cdc,schema)

  df_cdc.createOrReplaceGlobalTempView(f"view_{table_formatted}")
  query = f'''
  WITH ranked_data AS (
    SELECT *,
          ROW_NUMBER() OVER (PARTITION BY {id_field} ORDER BY {timestamp_field} DESC) AS rn
    FROM global_temp.view_{table_formatted}
  )
  SELECT *
  FROM ranked_data
  WHERE rn = 1;
  '''
  df_cdc = spark.sql(query)
  df_cdc = df_cdc.drop("rn")

  deltatable = DeltaTable.forName(spark,table_name)

  (deltatable
      .alias("b")
      .merge(df_cdc.alias("d"), f"b.{id_field} = d.{id_field}")
      .whenMatchedDelete(condition = "d.operacao = 'DELETE'")
      .whenMatchedUpdateAll(condition = "d.operacao = 'UPDATE'")
      .whenNotMatchedInsertAll(condition = "d.operacao = 'INSERT' OR d.operacao = 'UPDATE'")
      .execute())

if not table_exists(catalog,table_formatted):
    ingest_full_load()
else:
    ingest_cdc()