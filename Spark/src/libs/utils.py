from pyspark.sql.functions import col
from pyspark.sql.types import *
import json
from pyspark.sql import types

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

def get_schema(catalog,table):
    with open(f'./{catalog}/table_schemas/{table}.json', 'r') as file:
        schema_json = json.load(file)
    return types.StructType.fromJson(schema_json)
