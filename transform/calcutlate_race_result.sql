-- Databricks notebook source
use f1_presentations ;


-- COMMAND ----------

create table calculate_point_of_driver_team
using parquet
as 
select 
driver_name, 
season , 
team , 
points , 
11-position as calculate_point 
from race_results
where position < 10

-- COMMAND ----------


