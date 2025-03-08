from libs.utils import convert_data_types,get_schema

class Ingestor():
    def __init__(self,spark,catalog,database,table,data_format):
        self.spark = spark
        self.catalog = catalog
        self.database = database
        self.table = table
        self.file_format = data_format
        self.set_schema()

        self.table_aux = self.table.replace(".", "_")
        self.table_formatted = f"{self.database}_{self.table_aux}"
        self.table_name = f"{self.catalog}.{self.table_formatted}"

    def set_schema(self):
        self.data_schema = get_schema()

    def load(self):
        raw = f's3a://raw/{self.database}/full_load/{self.table}/'
        return (self.spark.read
        .format(self.file_format)
        .load(raw)
        )
    
    def save(self,df):
        (df.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(self.table_name))

    def execute(self):
        df = convert_data_types(self.load(),self.data_schema)
        self.save(df)

# def ingest_cdc():
#   print(f"ingesting {table_formatted} in cdc")
#   raw = f's3a://raw/{database}/cdc/{table}/'
#   df_cdc = (spark.read
#                 .format(file_format)
#                 .load(raw)
#         )
#   df_cdc = convert_data_types(df_cdc,schema)

#   df_cdc.createOrReplaceGlobalTempView(f"view_{table_formatted}")
#   query = f'''
#   WITH ranked_data AS (
#     SELECT *,
#           ROW_NUMBER() OVER (PARTITION BY {id_field} ORDER BY {timestamp_field} DESC) AS rn
#     FROM global_temp.view_{table_formatted}
#   )
#   SELECT *
#   FROM ranked_data
#   WHERE rn = 1;
#   '''
#   df_cdc = spark.sql(query)
#   df_cdc = df_cdc.drop("rn")

#   deltatable = DeltaTable.forName(spark,table_name)

#   (deltatable
#       .alias("b")
#       .merge(df_cdc.alias("d"), f"b.{id_field} = d.{id_field}")
#       .whenMatchedDelete(condition = "d.operacao = 'DELETE'")
#       .whenMatchedUpdateAll(condition = "d.operacao = 'UPDATE'")
#       .whenNotMatchedInsertAll(condition = "d.operacao = 'INSERT' OR d.operacao = 'UPDATE'")
#       .execute())