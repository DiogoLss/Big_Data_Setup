import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col

spark = SparkSession.builder \
    .appName("Spark Session") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql(f"CREATE DATABASE IF NOT EXISTS silver")
df = spark.sql("REFRESH TABLE bronze.nubank_extrato")
df = spark.sql("REFRESH TABLE bronze.nubank_fatura")

df = spark.sql("""
SELECT 
DISTINCT
Identificador, 
cast(to_date(from_unixtime(unix_timestamp(Data, 'dd/MM/yyyy'))) as date) AS Data,
cast(to_date(from_unixtime(unix_timestamp(Data, 'dd/MM/yyyy'))) as date) AS Data_Pgto,
Descricao,
'Debito' AS DebitoCredito,
CASE WHEN Valor < 0 THEN 'PAGO' ELSE 'RECEBIDO' END AS Tipo,
Valor
FROM bronze.nubank_extrato
""")

pattern = r"(?<= - )([^-\d]+)"

df = df.withColumn("Entidade", regexp_extract(col("Descricao"), pattern, 1))

df.coalesce(1).write.mode("overwrite").format("parquet").saveAsTable(f"silver.nubank_extrato")

df = spark.sql("""
SELECT
DISTINCT
date AS Data,
CAST(SUBSTRING(arquivo_origem,8,10) AS date) AS Data_Pgto,
title AS Descricao,
'Credito' AS DebitoCredito,
'PAGO' AS Tipo,
(amount * -1) AS Valor
FROM bronze.nubank_fatura
""")

pattern = r"^(.*?)(?: - |$)"

df = df.withColumn("Entidade", regexp_extract(col("Descricao"), pattern, 1))

df.coalesce(1).write.mode("overwrite").format("parquet").saveAsTable(f"silver.nubank_fatura")