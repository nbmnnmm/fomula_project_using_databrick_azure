# Databricks notebook source
# MAGIC %run ../include/config_param

# COMMAND ----------

# MAGIC %run ../include/config_function

# COMMAND ----------

from pyspark.sql.types import StructField , StructType , IntegerType , DoubleType , StringType
from pyspark.sql.functions import current_timestamp , lit , col , to_timestamp

# COMMAND ----------

df_pitstop = spark.read.parquet(f'{path_to_file_raw}/pitstop')

# COMMAND ----------

df_pitstop1 = df_pitstop.withColumnRenamed('driverId','driver_id') \
                        .withColumn('ingestion_date', lit(current_timestamp()))

# COMMAND ----------

df_races = df_races = spark.read.parquet(f'{path_to_process}/races')
df_pitstop2 = df_races.select('date','race_id').join(df_pitstop1 , df_races.date == df_pitstop1.date).drop('date')

# COMMAND ----------

df_pitstop2 = df_pitstop2.drop('circuitId','raceName','season','round')

# COMMAND ----------

df_pitstop2.show()

# COMMAND ----------

df_pitstop2 = df_pitstop2.withColumn('lap',col('lap').cast(IntegerType()))\
.withColumn('stop',col('stop').cast(IntegerType()))

# COMMAND ----------

df_pitstop2.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pitstop')

# COMMAND ----------

dbutils.notebook.exit('success')
