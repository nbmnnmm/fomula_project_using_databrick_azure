# Databricks notebook source
import requests
import json

# COMMAND ----------

data_1 = requests.get('https://ergast.com/api/f1/constructors.json').text
data_2 = json.loads(data1)

# COMMAND ----------

data_3 = data_2['MRData']['ConstructorTable']['Constructors']

# COMMAND ----------

df_constructors = spark.createDataFrame(data_3)

# COMMAND ----------

df_constructors.show()

# COMMAND ----------

df_constructors.write.mode('overwrite').format('parquet').saveAsTable('f1_raws.constructors')

# COMMAND ----------


