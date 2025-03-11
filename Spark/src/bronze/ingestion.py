from libs.spark_session import Spark
from libs.utils import *
from libs.ingestors import Ingestor,IngestorCDC

spark = Spark().spark

catalog = 'bronze'
database = 'postgres'
table = 'financeiro.clientes'
data_format = 'parquet'
source_as_string = 'true'
id_field = 'cliente_id'
timestamp_field = 'modificado_em'

table_aux = table.replace(".", "_")
table_formatted = f"{database}_{table_aux}"

fullload_source = f's3a://raw/{database}/full_load/{table}/'
cdc_source = f's3a://raw/{database}/cdc/{table}/'
checkpoint_location = f's3a://raw/{database}/cdc/{table}_checkpoint/'

if not table_exists(spark,catalog,table_formatted):
    print(f"ingesting table {table_formatted}")
    ingestor = Ingestor(
    spark,
    catalog,
    database,
    table,
    data_format
    )
    ingestor.execute(raw_source=fullload_source)
else:
    print(f"ingesting table using cdc {table_formatted}")
    ingestor = IngestorCDC(
        spark,
        catalog,
        database,
        table,
        data_format,
        source_as_string,
        checkpoint_location,
        id_field,
        timestamp_field
    )
    stream = ingestor.execute(raw_source=cdc_source)