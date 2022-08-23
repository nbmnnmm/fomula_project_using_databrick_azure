# Databricks notebook source
# MAGIC %run ../include/config_param

# COMMAND ----------

# MAGIC %run ../include/config_function

# COMMAND ----------

import time
from datetime import datetime 
from pyspark.sql.functions import col,lit
from pyspark.sql.types import StructType , StructField , IntegerType , StringType , DoubleType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df_circuits = spark.read.parquet(f'{path_to_file_raw}/circuits')
df_circuits.show()

# COMMAND ----------

exist = ['circuitId','circuitName','lat','long']
new = ['circuit_id','circuit_name','latitude','longtitude']
for i in range(len(new)):
    df_circuits = df_circuits.withColumnRenamed(exist[i],new[i])

# COMMAND ----------

df_circuits = df_circuits.withColumn('ingestion_date',current_timestamp()).drop('url')
df_circuits.show()

# COMMAND ----------

df_circuits.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.circuits') 

# COMMAND ----------

dbutils.notebook.exit('success')

# COMMAND ----------

 
