-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculate_point) AS total_points,
       AVG(calculate_point) AS avg_points,
       RANK() OVER(ORDER BY AVG(calculate_point) DESC) driver_rank
  FROM f1_presentations.calculate_point_of_driver_team
GROUP BY driver_name
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT season, https://adb-5609759552789363.3.azuredatabricks.net/?o=5609759552789363#
       driver_name,
       COUNT(1) AS total_races,
       SUM(calculate_point) AS total_points,
       AVG(calculate_point) AS avg_points
  FROM f1_presentations.calculate_point_of_driver_team
 WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY season, driver_name
ORDER BY season, avg_points DESC

-- COMMAND ----------

SELECT season, 
       driver_name,
       COUNT(1) AS total_races,
       SUM(calculate_point) AS total_points,
       AVG(calculate_point) AS avg_points
  FROM f1_presentations.calculate_point_of_driver_team
 WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY season, driver_name
ORDER BY season, avg_points DESC

-- COMMAND ----------


