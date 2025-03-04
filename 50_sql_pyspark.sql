-- CREATE TABLE penetration_analysis_Meta_50 ( country VARCHAR(20), last_active_date DATETIME, listening_hours BIGINT, sessions BIGINT, user_id BIGINT);

-- INSERT INTO penetration_analysis_Meta_50 (country, last_active_date, listening_hours, sessions, user_id) VALUES ('USA', '2024-01-25', 15, 7, 101), ('USA', '2023-12-20', 5, 3, 102), ('USA', '2024-01-20', 25, 10, 103), ('India', '2024-01-28', 12, 6, 201), 
-- ('India', '2023-12-15', 8, 4, 202), ('India', '2024-01-15', 20, 7, 203), ('UK', '2024-01-29', 18, 9, 301), ('UK', '2023-12-30', 9, 4, 302), ('UK', '2024-01-22', 30, 12, 303), ('Canada', '2024-01-01', 11, 6, 401), 
-- ('Canada', '2023-11-15', 3, 2, 402), ('Canada', '2024-01-15', 22, 8, 403), ('Germany', '2024-01-10', 14, 7, 501), ('Germany', '2024-01-30', 10, 5, 502), ('Germany', '2024-01-01', 5, 3, 503);

WITH au as (
SELECT country,COUNT(*) as 'active_users'
FROM penetration_analysis_Meta_50
WHERE listening_hours > 10 AND sessions > 5 AND last_active_date > 2024-01-01
group by country
-- last_active_date > DATE_ADD(current_date(),INTERVAL -30  DAY)
), tu as (
SELECT country,COUNT(*) as 'total_users'
FROM penetration_analysis_Meta_50
group by country
)
SELECT a.country,ROUND((active_users/total_users)*100,2) as 'Active_User_Penetration_Rate'
FROM au a
JOIN tu t
ON a.country = t.country





from pyspark.sql import functions as F

au = penetration_analysis_Meta_50.groupBy(F.col("country"))\
	.agg(F.count("*").alias("active_users"))\
	.filter( (F.col("listening_hours")) > 10 & (F.col("sessions") > 5) & (F.col("last_active_date") > 2024-01-01) )\
    .select("country","active_users")
    
tu = penetration_analysis_Meta_50.groupBy(F.col("country"))\
	.agg(F.count("*").alias("total_users"))\
    .select("country","total_users")
    
temp = au.join(tu,on="country",how="inner").select(au["country"],au["active_users"],tu["total_users"])

temp.round((F.col("active_users")/F.col("total_users"))*100),2).alias("Active_User_Penetration_Rate")\
	.select("country","Active_User_Penetration_Rate")\
    .show()
