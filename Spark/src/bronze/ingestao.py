import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract,input_file_name

spark = SparkSession.builder \
    .appName("Spark Session") \
    .enableHiveSupport() \
    .getOrCreate()

def ingest(spark:SparkSession, projeto, table, format, header,schema,partition):
    """
    projeto == database
    """
    raw_path = f"s3a://raw/{projeto}/{table}/*"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze")

    df = spark.read.format(format).option('header', header).schema(schema).load(raw_path).withColumn("arquivo_origem",  regexp_extract(input_file_name(), "([^/]+)$", 1))
    if partition == 'None':
        df.coalesce(1).write.mode("overwrite").format("parquet").saveAsTable(f"bronze.{projeto}_{table}")
    else:
        df.write.partitionBy(partition).mode("overwrite").format("parquet").saveAsTable(f"bronze.{projeto}_{table}")

ingest(
    spark,
    sys.argv[1],#projeto
    sys.argv[2],#tabela
    sys.argv[3],#formato-fonte
    sys.argv[4],#schema
    sys.argv[5],#header
    sys.argv[6]#partition
)