# Databricks notebook source
# MAGIC %run ../include/config_param

# COMMAND ----------

# MAGIC %run ../include/config_function

# COMMAND ----------

from pyspark.sql.types import StructType , StructField , IntegerType , StringType , FloatType 
from pyspark.sql.functions import col , lit , current_timestamp , monotonically_increasing_id

# COMMAND ----------

df_result = spark.read \
.parquet(f'{path_to_file_raw}/results')

# COMMAND ----------

df_result.select('date').distinct().show()

# COMMAND ----------

df_result1 = df_result.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumn('ingestion_date',lit(current_timestamp()))\
                                    .withColumn('result_id',monotonically_increasing_id())\
                                    .drop('raceName','circuitId')

# COMMAND ----------

df_races = df_races = spark.read.parquet(f'{path_to_process}/races')
df_result2 = df_races.select('date','race_id').join(df_result1 , df_races.date == df_result1.date).drop('date')

# COMMAND ----------

df_result2.count()

# COMMAND ----------

df_result3 = df_result2.withColumn('position',col('position').cast(IntegerType()))\
.withColumn('points',col('points').cast(IntegerType()))\
.withColumn('number',col('number').cast(IntegerType()))\
.withColumn('grid',col('grid').cast(IntegerType()))\
.withColumn('laps',col('laps').cast(IntegerType()))\
.withColumn('round',col('round').cast(IntegerType()))\
.withColumn('season',col('season').cast(IntegerType()))

# COMMAND ----------

df_result3.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

dbutils.notebook.exit('success')
