# Databricks notebook source
# MAGIC %run ../include/config_param

# COMMAND ----------

# MAGIC %run ../include/config_function

# COMMAND ----------

from pyspark.sql.types import StructType, StructField , IntegerType , StringType , DoubleType
from pyspark.sql.functions import current_timestamp , lit

# COMMAND ----------

df_constructor = spark.read.parquet(f'{path_to_file_raw}/constructors')

# COMMAND ----------

df_constructor = df_constructor.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("name", "constructor_name") \
                                             .withColumn("ingestion_date", current_timestamp())\
                                             .drop('url')\
                                

# COMMAND ----------

 df_constructor.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.constructors')

# COMMAND ----------


