from libs.utils import convert_data_types, get_schema, set_schema_string
from delta import *

class Ingestor():
    def __init__(self,spark,catalog,database,table,file_format):
        self.spark = spark
        self.catalog = catalog
        self.database = database
        self.table = table
        self.file_format = file_format

        self.table_aux = self.table.replace(".", "_")
        self.table_formatted = f"{self.database}_{self.table_aux}"
        self.table_name = f"{self.catalog}.{self.table_formatted}"
        self.set_schema()

    def set_schema(self):
        self.data_schema = get_schema(self.catalog,self.table_formatted)

    def load(self,path):
        return convert_data_types(self.spark.read
        .format(self.file_format)
        .load(path),self.data_schema
        )
    
    def save(self,df):
        (df.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(self.table_name))

    def execute(self,raw_source):
        df = self.load(raw_source)
        self.save(df)

class IngestorCDC(Ingestor):
    def __init__(self, spark, catalog, database, table, file_format,source_as_string,checkpoint_location,id_field,timestamp_field):
        super().__init__(spark, catalog, database, table, file_format)
        self.source_as_string = source_as_string
        self.checkpoint_location = checkpoint_location
        self.id_field = id_field
        self.timestamp_field = timestamp_field

        self.set_delta_table()

    def set_delta_table(self):
        self.deltatable = DeltaTable.forName(self.spark,self.table_name)

    def load(self,path):
        read_schema = self.data_schema
        if self.source_as_string == 'true':
            read_schema = set_schema_string(read_schema)
        return (self.spark.readStream
        .schema(read_schema)
        .format(self.file_format)
        .load(path))

    def upsert(self, df):
        print(self.data_schema)
        df = convert_data_types(df,self.data_schema)
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
    
    def save(self,df_stream):
        stream = (df_stream.writeStream
        .option("checkpointLocation",self.checkpoint_location)
        .foreachBatch(lambda df, batch_id: self.upsert(df))
        .trigger(availableNow=True))
        return stream.start()
