# Databricks notebook source
# MAGIC %run ../include/config_param

# COMMAND ----------

# MAGIC %run ../include/config_function

# COMMAND ----------


from pyspark.sql.functions import lit, current_timestamp

# COMMAND ----------

df_results = spark.read.parquet(f"{path_to_process}/results")
df_races = spark.read.parquet(f"{path_to_process}/races")
df_circuits = spark.read.parquet(f"{path_to_process}/circuits")
df_constructor = spark.read.parquet(f"{path_to_process}/constructors")
df_drivers = spark.read.parquet(f"{path_to_process}/driver")

# COMMAND ----------

df_results

# COMMAND ----------

df_all = df_results \
.join(df_races,df_results.race_id == df_races.race_id) \
.join(df_drivers , df_results.driver_id == df_drivers.driver_id , 'leftouter') \
.join(df_constructor , df_results.constructor_id == df_constructor.constructor_id,'leftouter') \
.join(df_circuits,df_races.circuit_id == df_circuits.circuit_id,'left')\
.select(df_races['season']\
       ,df_races['race_name']\
        ,df_races['time']\
        ,df_races['date']\
        ,df_circuits['country'].alias('circuit_location')\
        ,df_drivers['name'].alias('driver_name')\
        ,df_drivers['permanentNumber'].alias('driver_number')\
        ,df_drivers['nationality'].alias('driver_nationality')\
        ,df_results['constructor_id'].alias('team')\
        ,df_results['grid']\
        ,df_results['fastest_lap_time']\
        ,df_results['points'] \
        ,df_results['position']\
        ,df_races['race_id']
       )\
.withColumn('created_date',lit(current_timestamp()))

# COMMAND ----------

df_all.write.mode('overwrite').format('parquet').saveAsTable('f1_presentations.race_results')

# COMMAND ----------


