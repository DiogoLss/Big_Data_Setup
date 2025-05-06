import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from libs.spark_session import Spark
from libs.utils import *
from libs.ingestors import Ingestor,IngestorCDC

def import_query(table_name):
    import os
    base_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(base_dir, 'queries',f'{table_name}.sql')
    schema_path = os.path.abspath(schema_path)
    with open(schema_path, 'r') as file:
        return file.read()

spark = Spark().spark

catalog = 'silver'
table_name = sys.argv[1]
query = import_query(table_name)

spark.sql(query).write.format("delta").mode("overwrite").option('overwriteSchema','true').saveAsTable(f'{catalog}.{table_name}')