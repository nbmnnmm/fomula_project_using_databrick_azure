# Databricks notebook source
# MAGIC %run ../include/config_param

# COMMAND ----------

# MAGIC %run ../include/config_function

# COMMAND ----------

from pyspark.sql.types import StructField , StructType , IntegerType , FloatType , StringType
from pyspark.sql.functions import current_timestamp , lit ,col


# COMMAND ----------

df_laptime1 = spark.read.parquet(f'{path_to_file_raw}/laptime')

# COMMAND ----------

df_laptime1.show()

# COMMAND ----------

df_races = spark.read.parquet(f'{path_to_process}/races')

# COMMAND ----------

df_laptime2 = df_races.select('date','race_id').join(df_laptime1 , df_races.date == df_laptime1.date).drop('date')

# COMMAND ----------

df_laptime3 = df_laptime2.withColumnRenamed('driverId','driver_id')\
.withColumn('ingestion_date',lit(current_timestamp()))\
.withColumnRenamed('numberLap','lap')

# COMMAND ----------

df_laptime4 = df_laptime3.withColumn('lap',col('lap').cast(IntegerType()))\
.withColumn('position',col('position').cast(IntegerType()))\
.drop('round')\
.drop('season')\
.drop('CircuitId')

# COMMAND ----------

df_laptime4.printSchema()

# COMMAND ----------

df_laptime4.write.format('parquet').saveAsTable('f1_processed.laptime')

# COMMAND ----------

dbutils.notebook.exit('success')
