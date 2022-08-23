-- Databricks notebook source
drop database if exists f1_processed cascade ;
drop database if exists f1_presentations cascade ; 
drop database if exists f1_raws cascade; 

-- COMMAND ----------

create database f1_processed 
location "/mnt/fomula/processed"

-- COMMAND ----------

create database f1_presentations 
location "/mnt/fomula/presentation"

-- COMMAND ----------

create database f1_raws 
location "/mnt/fomula/raw"

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC ls /mnt/fomula/raw

-- COMMAND ----------


