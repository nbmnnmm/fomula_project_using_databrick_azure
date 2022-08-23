# Databricks notebook source
# MAGIC %run ../include/config_param

# COMMAND ----------

# MAGIC %run ../include/config_function

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import Window

# COMMAND ----------

df_race_result = spark.read.parquet(f'{path_to_presentation}/race_results')

# COMMAND ----------

df_constructor_standing = df_race_result.groupBy('season' , 'team' )\
.agg(f.count(f.when(f.col('position') == 1 , True)).alias('win') , f.sum('points').alias('total_point'))

# COMMAND ----------

window_spec = Window.partitionBy('season').orderBy(f.desc(f.col('total_point')) , f.desc(f.col('win')))

# COMMAND ----------

df_constructor_standing1 = df_constructor_standing.withColumn('rank' , f.rank().over(window=window_spec))

# COMMAND ----------

df_constructor_standing1.write.mode('overwrite').format('parquet').saveAsTable('f1_presentations.construct_standing')

# COMMAND ----------


