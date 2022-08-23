# Databricks notebook source
dbutils.widgets.text('file_date','')
file_date = dbutils.widgets.get('file_date')

# COMMAND ----------

v_res = dbutils.notebook.run('ingestion_circuit_file',0,{'file_date' : file_date})
v_res

# COMMAND ----------

v_res = dbutils.notebook.run('ingestion_constructor_file',0,{'file_date' : file_date})
v_res

# COMMAND ----------

v_res = dbutils.notebook.run('ingestion_lap_time_folder',0,{'file_date' : file_date})
v_res

# COMMAND ----------

v_res = dbutils.notebook.run('ingestion_pitstop_file',0,{'file_date' : file_date})
v_res

# COMMAND ----------

v_res = dbutils.notebook.run('ingestion_qualifying_folder',0,{'file_date' : file_date})
v_res

# COMMAND ----------

v_res = dbutils.notebook.run('ingestion_race_file',0,{'file_date' : file_date})
v_res

# COMMAND ----------

v_res = dbutils.notebook.run('ingestion_result_file',0,{'file_date' : file_date})
v_res

# COMMAND ----------

v_res = dbutils.notebook.run('ingestion_driver_file',0,{'file_date' : file_date})
v_res
