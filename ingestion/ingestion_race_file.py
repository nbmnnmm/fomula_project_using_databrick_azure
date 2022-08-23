# Databricks notebook source
# MAGIC %run ../include/config_param

# COMMAND ----------

# MAGIC %run ../include/config_function

# COMMAND ----------

from pyspark.sql.types import StructType , StructField , IntegerType , StringType , DoubleType
from pyspark.sql.functions import col , lit , to_timestamp , concat , current_timestamp

# COMMAND ----------

df_races = spark.read.parquet(f'{path_to_file_raw}/races')

# COMMAND ----------

if 'url' in df_races.columns:
    df_races = df_races.drop('url')

# COMMAND ----------

exist = ['raceId','year','circuitId','raceName'] 
new = ['race_id','race_year','circuit_id','race_name']
for i in range(4) : 
    df_races = df_races.withColumnRenamed(exist[i],new[i])

df_races = df_races.withColumn('ingestion_date',lit(current_timestamp()))

# COMMAND ----------

df_races.show()

# COMMAND ----------

df_races1 = df_races.withColumn('round',col('round').cast(IntegerType()))\
.withColumn('season',col('season').cast(IntegerType()))

# COMMAND ----------

df_races1.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.races')

# COMMAND ----------

dbutils.notebook.exit('success')
