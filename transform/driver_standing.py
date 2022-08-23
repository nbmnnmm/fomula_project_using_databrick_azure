# Databricks notebook source
# MAGIC %run ../include/config_param

# COMMAND ----------

# MAGIC %run ../include/config_function

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

df_race_result = spark.read.parquet(f'{path_to_presentation}/race_results')

# COMMAND ----------

df_driver_standing = df_race_result.groupBy('driver_nationality' , 'season' , 'team')\
.agg(F.sum('points').alias('total_point') , F.count(F.when(df_race_result['position']==1 , True)).alias('win'))

# COMMAND ----------

window_spec = Window.partitionBy('season').orderBy(F.desc('total_point'),F.desc('win'))

# COMMAND ----------

df_driver_standing = df_driver_standing.withColumn('rank',F.rank().over(window=window_spec))

# COMMAND ----------

df_driver_standing.write.mode('overwrite').format('parquet').saveAsTable('f1_presentations.driver_standing')

# COMMAND ----------


