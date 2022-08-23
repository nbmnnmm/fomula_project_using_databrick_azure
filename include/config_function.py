# Databricks notebook source
def incremental_load(df_input , database , table , partition_column ):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if(spark._jsparkSession.catalog().tableExists(f'{database}.{table}') == False):
        df_input.write.mode('overwrite').partitionBy(partition_column).format('parquet').saveAsTable(f'{database}.{table}')
    else :
        df_input.write.mode('overwrite').insertInto(f'{database}.{table}')

# COMMAND ----------

def re_arrange_column(df_input , partition_column):
    require_columm = []
    print(type(df_input))
    for i in df_input.columns : 
        if i != partition_column : 
            require_columm.append(i)
    require_columm.append(partition_column)
    return df_input.select(require_columm)
