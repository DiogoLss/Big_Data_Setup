from libs.utils import convert_data_types,get_schema
from delta import *

class Ingestor():
    def __init__(self,spark,catalog,database,table,file_format):
        self.spark = spark
        self.catalog = catalog
        self.database = database
        self.table = table
        self.file_format = file_format
        self.set_schema()

        self.table_aux = self.table.replace(".", "_")
        self.table_formatted = f"{self.database}_{self.table_aux}"
        self.table_name = f"{self.catalog}.{self.table_formatted}"

    def set_schema(self):
        self.data_schema = get_schema()

    def load(self):
        raw = f's3a://raw/{self.database}/full_load/{self.table}/'
        return convert_data_types(self.spark.read
        .format(self.file_format)
        .load(raw),self.data_schema
        )
    
    def save(self,df):
        (df.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(self.table_name))

    def execute(self):
        df = self.load()
        self.save(df)

class IngestorCDC(Ingestor):
    def __init__(self, spark, catalog, database, table, file_format,id_field,timestamp_field):
        super().__init__(spark, catalog, database, table, file_format)
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.set_schema()
        self.set_delta_table()

    def set_delta_table(self):
        self.deltatable = DeltaTable.forName(self.spark,self.table_name)

    def upsert(self, df):
        df.createOrReplaceGlobalTempView(f"view_{self.table_formatted}")
        query = f'''
        WITH ranked_data AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY {self.id_field} ORDER BY {self.timestamp_field} DESC) AS rn
            FROM global_temp.view_{self.table_formatted}
        )
        SELECT *
        FROM ranked_data
        WHERE rn = 1;
        '''
        df = self.spark.sql(query)
        df = df.drop("rn")

        (self.deltatable
        .alias("b")
        .merge(df.alias("d"), f"b.{self.id_field} = d.{self.id_field}")
        .whenMatchedDelete(condition = "d.operacao = 'DELETE'")
        .whenMatchedUpdateAll(condition = "d.operacao = 'UPDATE'")
        .whenNotMatchedInsertAll(condition = "d.operacao = 'INSERT' OR d.operacao = 'UPDATE'")
        .execute())

    def load(self):
        raw = f's3a://raw/{self.database}/cdc/{self.table}/'
        return convert_data_types(self.spark.read
        .format(self.file_format)
        .load(raw),self.data_schema
        )
    
    def save(self):
        df = self.load()
        self.upsert(df)
