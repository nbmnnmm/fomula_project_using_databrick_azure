-- Databricks notebook source
use f1_presentations ; 

-- COMMAND ----------

select team  , 
sum(calculate_point) as total_point , 
avg(calculate_point) as  avg_point ,
count(1) as total_race 
from calculate_point_of_driver_team 
group by team  
order by avg_point;

-- COMMAND ----------


