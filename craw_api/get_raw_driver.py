# Databricks notebook source
import requests 
import json 

# COMMAND ----------

data_1 = requests.get('https://ergast.com/api/f1/drivers.json').text
data_2 = json.loads(data_1)

# COMMAND ----------

df_driver = spark.createDataFrame(data_2['MRData']['DriverTable']['Drivers'])

# COMMAND ----------

df_driver.show()

# COMMAND ----------

df_driver.write.mode('overwrite').format('parquet').saveAsTable('f1_raws.driver')

# COMMAND ----------


