# Databricks notebook source
import requests
import json 
from pyspark.sql.functions import lit , col 
from pyspark.sql.types import * 

# COMMAND ----------

schema_timings = StructType([
    StructField('driverId',StringType()),
    StructField('position',StringType()),
    StructField('time',StringType())
])

# COMMAND ----------

init = True
season = 2020
while season <= 2022 : 
    round = 1
    while 1 : 
        lap_num = 1
        url = f'https://ergast.com/api/f1/{season}/{round}/laps/{lap_num}.json'
        data = requests.get(url).text
        data1 = json.loads(data)
        data2 = data1['MRData']['RaceTable']['Races']
        if len(data2) == 0 : 
            break 
        while 1 : 
            df_temp = spark.createDataFrame(data2[0]['Laps'][0]['Timings'],schema=schema_timings).withColumn('round',lit(data2[0]['round']))\
            .withColumn('numberLap' , lit(data2[0]["Laps"][0]['number']))\
            .withColumn('season',lit(data2[0]['season']))\
            .withColumn('circuitId',lit(data2[0]['Circuit']['circuitId']))\
            .withColumn('date',lit(data2[0]['date']))
            if(init == True):
                df_laptimes = df_temp 
                init = False
            else:
                df_laptimes = df_laptimes.union(df_temp)
            lap_num += 1
            url = f'https://ergast.com/api/f1/{season}/{round}/laps/{lap_num}.json'
            data = requests.get(url).text
            data1 = json.loads(data)
            data2 = data1['MRData']['RaceTable']['Races']
            print(f'{season}-{round}-{lap_num}')
            if len(data2) == 0 : 
                break
        round += 1
    season += 1

# COMMAND ----------

df_laptimes.write.mode('overwrite').format('parquet').saveAsTable('f1_raws.laptime')

# COMMAND ----------


