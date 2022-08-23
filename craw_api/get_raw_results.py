# Databricks notebook source
# MAGIC %run ../include/config_function

# COMMAND ----------

# MAGIC %run ../include/config_param

# COMMAND ----------

import requests 
import json 
from pyspark.sql.types import *
from pyspark.sql.functions import col , explode , lit

# COMMAND ----------

driver_schema = StructType([
    StructField('driverId',StringType(),True),
    StructField('code',StringType()),
    StructField('url',StringType(),True),
    StructField('givenName',StringType(),True),
    StructField('familyName',StringType(),True),
    StructField('dateOfBirth',StringType(),True),
    StructField('nationality',StringType(),True)
])

constructor_schema = StructType([
    StructField('constructorId',StringType(),True),
    StructField('url',StringType(),True),
    StructField('name',StringType(),True),
    StructField('nationality',StringType(),True),
])
time_schema = StructType([
    StructField('millis',StringType(),True),
    StructField('time',StringType(),True)
])
fastest_time_schema = StructType([
    StructField('time',StringType(),True)
])
fastest_schema = StructType([
    StructField('rank',StringType(),True),
    StructField('lap',StringType(),True),
    StructField('Time',fastest_time_schema),
])

results_schema =StructType([
    StructField('number',StringType(),True),
    StructField('position',StringType(),True),
    StructField('positionText',StringType(),True),
    StructField('points',StringType(),True),
    StructField('Driver',driver_schema,True),
    StructField('Constructor',constructor_schema,True),
    StructField('grid',StringType(),True),
    StructField('laps',StringType(),True),
    StructField('status',StringType(),True),
    StructField('Time',time_schema),
    StructField('FastestLap',fastest_schema)
]
)

# COMMAND ----------

season = 2020
init = True
while season <= 2022: 
    round = 1
    while 1 : 
        data = requests.get(f'https://ergast.com/api/f1/{season}/{round}/results.json').text
        data1 = json.loads(data)
        data2 = data1['MRData']['RaceTable']['Races']
        if(len(data2) == 0) :
            break 
        df_temp = spark.createDataFrame(data2[0]['Results'],schema=results_schema)\
        .withColumn('round',lit(data2[0]['round']))\
        .withColumn('circuitId',lit(data2[0]['Circuit']['circuitId']))\
        .withColumn('season',lit(data2[0]['season']))\
        .withColumn('raceName',lit(data2[0]['raceName']))\
        .withColumn('date',lit(data2[0]['date']))
        if (init == True):
            df_results = df_temp 
            init = False 
        else :
            df_results = df_results.union(df_temp)
        print(season,round)
        round +=1 
    season += 1 
print('finish')
    
    

# COMMAND ----------

df_results1 = df_results.withColumn('driverId',col('Driver.driverId'))\
.withColumn('ConstructorId',col('Constructor.constructorId'))\
.withColumn('millis',col('Time.millis'))\
.withColumn('tm',col('Time.time'))\
.withColumn('FastestLapTime',col('FastestLap.Time.time'))

# COMMAND ----------

df_results2 = df_results1.drop('Driver','Constructor','Time','FastestLap')

# COMMAND ----------

df_results2.write.mode('overwrite').format('parquet').saveAsTable('f1_raws.results')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_raws.results

# COMMAND ----------


