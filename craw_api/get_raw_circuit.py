# Databricks notebook source
import requests
import json

# COMMAND ----------

data_1 = requests.get('https://ergast.com/api/f1/circuits.json').text

# COMMAND ----------

data_2 = json.loads(data_1)

# COMMAND ----------

data_3 = data_2['MRData']['CircuitTable']['Circuits']

# COMMAND ----------

df_circuit = spark.createDataFrame(data=data_3)

# COMMAND ----------

df_circuit1 = df_circuit.withColumn('long',df_circuit['location']['long'])\
.withColumn('lat',df_circuit['location']['lat'])\
.withColumn('locality',df_circuit['location']['locality'])\
.withColumn('country',df_circuit['location']['country'])\
.drop('location')

# COMMAND ----------

df_circuit1.write.mode('overwrite').format('parquet').saveAsTable('f1_raws.circuits')

# COMMAND ----------


