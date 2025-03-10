from pyspark.sql.functions import col
from pyspark.sql.types import *

def set_schema_string(schema):
    for type in schema.fields:
        type.dataType = StringType()
    return schema

def convert_data_types(df,schema):
    schema_dict = {field.name: field.dataType for field in schema.fields}
    for column_name, data_type in schema_dict.items():
        if column_name in df.columns:
            df = df.withColumn(column_name, col(column_name).cast(data_type))
    return df

def table_exists(spark,catalog,table):
    df = spark.sql(f"show tables from {catalog}")
    df = df.filter(df.tableName == table)
    df = df.count()
    return df > 0


def get_schema():
    return StructType([
            StructField("cliente_id", LongType(), True),
            StructField("nome", StringType(), True),
            StructField("cidade", StringType(), True),
            StructField("modificado_em", TimestampType(), True),
            StructField("operacao", StringType(), True)
        ])
    #     schema = StructType([
    #     StructField("id", LongType(), True),
    #     StructField("data_movimento", DateType(), True),
    #     StructField("valor", DoubleType(), True),
    #     StructField("cliente_id", LongType(), True),
    #     StructField("modificado_em", TimestampType(), True)
    # ])
