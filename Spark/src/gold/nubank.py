from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark Session") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.sql("REFRESH TABLE silver.nubank_extrato")
df = spark.sql("REFRESH TABLE silver.nubank_fatura")

df = spark.sql("""
SELECT
Data,
Data_Pgto,
DebitoCredito,
Entidade,
Tipo,
Valor
FROM silver.nubank_extrato        
WHERE Identificador NOT IN
(
    SELECT e.Identificador FROM silver.nubank_extrato e
    INNER JOIN 
          (
          SELECT DISTINCT Data_Pgto FROM silver.nubank_fatura 
          WHERE Descricao != 'Pagamento recebido'
          ) f ON SUBSTRING(f.Data_Pgto,0,7) = SUBSTRING(e.Data,0,7)
    WHERE e.Descricao = 'Pagamento de fatura'
)
UNION ALL
SELECT 
Data,
Data_Pgto,
DebitoCredito,
Entidade,
Tipo,
Valor
FROM silver.nubank_fatura
Where Descricao != 'Pagamento recebido'
""")

df.coalesce(1).write.mode("overwrite").format("parquet").saveAsTable(f"gold.nubank_movimentacao")