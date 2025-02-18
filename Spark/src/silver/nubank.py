import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark Session") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql(f"CREATE DATABASE IF NOT EXISTS silver")

df = spark.sql("""
SELECT 
DISTINCT
Identificador, 
cast(to_date(from_unixtime(unix_timestamp(Data, 'dd/MM/yyyy'))) as date) AS Data,
cast(to_date(from_unixtime(unix_timestamp(Data, 'dd/MM/yyyy'))) as date) AS Data_Pgto,
Descricao,
CASE WHEN Valor < 0 THEN 'PAGO' ELSE 'RECEBIDO' END AS Tipo,
Valor
FROM bronze.nubank_extrato
""")

df.coalesce(1).write.mode("overwrite").format("parquet").saveAsTable(f"silver.nubank_extrato")

df = spark.sql("""
SELECT
DISTINCT
date AS Data,
CAST(SUBSTRING(arquivo_origem,8,10) AS date) AS Data_Pgto,
title AS Descricao,
'PAGO' AS Tipo,
(amount * -1) AS Valor
FROM bronze.nubank_fatura
""")

df.coalesce(1).write.mode("overwrite").format("parquet").saveAsTable(f"silver.nubank_fatura")