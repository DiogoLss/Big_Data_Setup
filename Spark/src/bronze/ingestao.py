from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

spark = SparkSession.builder \
    .appName("Spark Session") \
    .enableHiveSupport() \
    .getOrCreate()

def ingest(spark, projeto, table, format, header='false',schema=None):
    """
    projeto == database
    """
    raw_path = f"s3a://raw/{projeto}/{table}/*"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze")

    df = spark.read.format(format).option('header', header).schema(schema).load(raw_path)
    df.write.mode("overwrite").format("parquet").saveAsTable(f"bronze.{projeto}_{table}")




schema = "Data STRING, Valor DOUBLE, Identificador STRING, Descricao STRING"
ingest(spark,'nubank','extrato','csv','true',schema)