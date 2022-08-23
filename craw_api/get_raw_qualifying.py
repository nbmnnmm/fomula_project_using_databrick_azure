# Databricks notebook source
import requests 
import json 
from pyspark.sql.types import *
from pyspark.sql.functions import col  , lit

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
schema_qualifying = StructType([
    StructField('number',StringType()),
    StructField('position',StringType()),
    StructField('Driver',driver_schema),
    StructField('Constructor',constructor_schema),
    StructField('Q1',StringType()),
    StructField('Q2',StringType()),
    StructField('Q3',StringType()),
])

# COMMAND ----------

season = 2020
init = True
while season <= 2022: 
    round = 1
    while 1 : 
        data = requests.get(f'https://ergast.com/api/f1/{season}/{round}/qualifying.json').text
        data1 = json.loads(data)
        data2 = data1['MRData']['RaceTable']['Races']
        if(len(data2) == 0) :
            break 
        df_temp = spark.createDataFrame(data2[0]['QualifyingResults'],schema=schema_qualifying)\
        .withColumn('round',lit(data2[0]['round']))\
        .withColumn('circuitId',lit(data2[0]['Circuit']['circuitId']))\
        .withColumn('season',lit(data2[0]['season']))\
        .withColumn('raceName',lit(data2[0]['raceName']))\
        .withColumn('date',lit(data2[0]['date']))
        if (init == True):
            df_qualifying = df_temp 
            init = False 
        else :
            df_qualifying = df_qualifying.union(df_temp)
        print(season,round)
        round +=1 
    season += 1 
print('finish')
    
    

# COMMAND ----------

df_qualifying1 = df_qualifying.withColumn('driverId',col('Driver.driverId'))\
.withColumn('constructorId',col('Constructor.constructorId'))\
.drop('Driver')\
.drop('Constructor')


# COMMAND ----------

df_qualifying1.write.mode('overwrite').format('parquet').saveAsTable('f1_raws.qualifying')

# COMMAND ----------


