# Databricks notebook source
# MAGIC %run ../include/config_param

# COMMAND ----------

# MAGIC %run ../include/config_function

# COMMAND ----------

from pyspark.sql.types import StructType,StructField , IntegerType , StringType
from pyspark.sql.functions import lit , current_timestamp , monotonically_increasing_id , col

# COMMAND ----------

df_qualifying = spark.read.parquet(f'{path_to_file_raw}/qualifying')

# COMMAND ----------

exist = ['qualifyId','driverId','constructorId']
new = ['qualify_id','driver_id','constructor_id']df_pitstop1
for i in range(3):
    df_qualifying = df_qualifying.withColumnRenamed(exist[i],new[i])

# COMMAND ----------

df_qualifying1 = df_qualifying.withColumn('ingestion_date',lit(current_timestamp()))

# COMMAND ----------

df_races = df_races = spark.read.parquet(f'{path_to_process}/races')
df_qualifying2 = df_races.select('date','race_id').join(df_qualifying1 , df_races.date == df_qualifying1.date).drop('date')

# COMMAND ----------

df_qualifying2.show()

# COMMAND ----------

df_qualifying3 = df_qualifying2.withColumn('qualifying_id', monotonically_increasing_id())\
.withColumn('number',col('number').cast(IntegerType()))\
.withColumn('position',col('position').cast(IntegerType()))\
.withColumn('round',col('round').cast(IntegerType()))\
.withColumn('season',col('season').cast(IntegerType()))\
.drop('raceName')

# COMMAND ----------

df_qualifying3.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

dbutils.notebook.exit('success')
