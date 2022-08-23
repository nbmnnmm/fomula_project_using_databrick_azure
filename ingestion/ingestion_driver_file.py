# Databricks notebook source
# MAGIC %run ../include/config_param

# COMMAND ----------

# MAGIC %run ../include/config_function

# COMMAND ----------

from pyspark.sql.types import StructType , StringType , FloatType , IntegerType , StructField
from pyspark.sql.functions import lit , current_timestamp , concat , col

# COMMAND ----------

df_driver1 = spark.read.parquet(f'{path_to_file_raw}/driver')

# COMMAND ----------

df_driver1.show()

# COMMAND ----------

df_driver2 = df_driver1.withColumnRenamed('driverId','driver_id') \
                        .withColumnRenamed('driverRef','driver_ref') \
                        .drop('url') \
                        .withColumn('ingestion_date',lit(current_timestamp())) \
                        .withColumn('name', concat(col('familyName'),lit(' '), col('givenName')))\
                        .drop('familyName','givenName')

# COMMAND ----------

df_driver2.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.driver')

# COMMAND ----------

dbutils.notebook.exit('success')
