-- Databricks notebook source
use f1_presentation ; 

-- COMMAND ----------

select 
driver_name , 
sum(calculate_point) as  total_point , 
count(1) as total_race , 
avg(calculate_point) as avg_point 
from calculate_point_of_driver_team 
where race_year between 2011 and 2020
group by driver_name 
having count(1) > 50
order by avg_point desc , total_point desc
;

-- COMMAND ----------


