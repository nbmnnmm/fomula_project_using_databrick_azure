# Databricks notebook source
import requests
import json 
from pyspark.sql.functions import lit , col 
from pyspark.sql.types import * 

# COMMAND ----------

schema = StructType([
    StructField('driverId',StringType()),
    StructField('lap',StringType()),
    StructField('stop',StringType()),
    StructField('time',StringType()),
    StructField('duration',StringType())
])


# COMMAND ----------

init = True
season = 2020
while season <= 2022 : 
    round = 1
    while 1 : 
        lap_num = 1
        url = f'https://ergast.com/api/f1/{season}/{round}/pitstops/{lap_num}.json'
        data = requests.get(url).text
        data1 = json.loads(data)
        data2 = data1['MRData']['RaceTable']['Races']
        if len(data2) == 0 : 
            break 
        while 1 : 
            df_temp = spark.createDataFrame(data2[0]['PitStops'],schema=schema)\
            .withColumn('round',lit(data2[0]['round']))\
            .withColumn('season',lit(data2[0]['season']))\
            .withColumn('circuitId',lit(data2[0]['Circuit']['circuitId']))\
            .withColumn('raceName',lit(data2[0]['raceName']))\
            .withColumn('date',lit(data2[0]['date']))
            if(init == True):
                df_pitstop = df_temp 
                init = False
            else:
                df_pitstop = df_pitstop.union(df_temp)
            print(f'{season}-{round}-{lap_num}')
            lap_num += 1
            url = f'https://ergast.com/api/f1/{season}/{round}/pitstops/{lap_num}.json'
            data = requests.get(url).text
            data1 = json.loads(data)
            data2 = data1['MRData']['RaceTable']['Races']
            if len(data2) == 0 : 
                break
        round += 1
    season += 1

# COMMAND ----------

df_pitstop.write.mode('overwrite').format('parquet').saveAsTable('f1_raws.pitstop')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_raws.pitstop
# MAGIC where season = 2021
# MAGIC limit 5

# COMMAND ----------


