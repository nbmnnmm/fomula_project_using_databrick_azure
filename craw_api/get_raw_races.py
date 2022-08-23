# Databricks notebook source
import requests 
import json 
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

circuit_schema = StructType([
    StructField('circuitId',StringType())
])
schema = StructType([
    StructField('season',StringType()),
    StructField('round',StringType()),
    StructField('url',StringType()),
    StructField('raceName',StringType()),
    StructField('Circuit',circuit_schema),
    StructField('date',StringType()),
    StructField('time',StringType())
])

# COMMAND ----------


season = 2020
init = True
while season <= 2022 :
    data = requests.get(f'https://ergast.com/api/f1/{season}.json').text
    data1 = json.loads(data)
    df_temp = spark.createDataFrame(data1['MRData']['RaceTable']['Races'],schema = schema)    
    if init == True :
        df_races = df_temp 
        init = False
    else :
        df_races = df_races.union(df_temp)
    print(season)
    season += 1 

# COMMAND ----------

df_races_1 = df_races.withColumn('circuitId',df_races['Circuit']['circuitId'])\
.drop('Circuit')

# COMMAND ----------

df_races_1 = df_races_1.withColumn('raceId',monotonically_increasing_id())

# COMMAND ----------

df_races_1.write.mode('overwrite').format('parquet').saveAsTable('f1_raws.races')

# COMMAND ----------

df_races_1.show()

# COMMAND ----------

df = spark.read.parquet(f'{path_to_file_raw}/races')

# COMMAND ----------

df.select('raceId').distinct().count()

# COMMAND ----------


