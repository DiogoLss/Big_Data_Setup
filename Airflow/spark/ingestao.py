from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

spark = SparkSession.builder \
    .appName("Spark Session") \
    .getOrCreate()

def ingest(spark, projeto, table, format, schema=None):
    raw = f"s3a://raw/{projeto}/{table}/*"
    bronze = f"s3a://data/bronze/{projeto}/{table}"

    df = spark.read.format(format).option('header','true').schema(schema).load(raw)
    df.write.mode('overwrite').save(bronze)

arqschema = "Data STRING, Valor DOUBLE, Identificador STRING, Descricao STRING"
# arqschema = "date DATE, title STRING, amount DOUBLE"
ingest(spark,'nubank','extrato','csv',arqschema)