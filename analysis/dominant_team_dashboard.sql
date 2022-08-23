-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team,
       COUNT(1) AS total_races,
       SUM(calculate_point) AS total_points,
       AVG(calculate_point) AS avg_points,
       RANK() OVER(ORDER BY AVG(calculate_point) DESC) team_rank
  FROM f1_presentations.calculate_point_of_driver_team
GROUP BY team
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT * FROM v_dominant_teams;

-- COMMAND ----------

SELECT season, 
       team,
       COUNT(1) AS total_races,
       SUM(calculate_point) AS total_points,
       AVG(calculate_point) AS avg_points
  FROM f1_presentations.calculate_point_of_driver_team
 WHERE team IN (SELECT team FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY season, team
ORDER BY season, avg_points DESC

-- COMMAND ----------

SELECT season, 
       team,
       COUNT(1) AS total_races,
       SUM(calculate_point) AS total_points,
       AVG(calculate_point) AS avg_points
  FROM f1_presentations.calculate_point_of_driver_team
 WHERE team IN (SELECT team FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY season, team
ORDER BY season, avg_points DESC

-- COMMAND ----------


