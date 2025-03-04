-- CREATE TABLE marathon_male_EY_46 (age BIGINT, div_tot TEXT, gun_time BIGINT, hometown TEXT, net_time BIGINT, num BIGINT, pace BIGINT, person_name TEXT, place BIGINT);

-- INSERT INTO marathon_male_EY_46 (age, div_tot, gun_time, hometown, net_time, num, pace, person_name, place) VALUES (25, '1/100', 3600, 'New York', 3400, 101, 500, 'John Doe', 1), (30, '2/100', 4000, 'Boston', 3850, 102, 550, 'Michael Smith', 2), (22, '3/100', 4200, 'Chicago', 4150, 103, 600, 'David Johnson', 3);

-- CREATE TABLE marathon_female_EY_46 (age BIGINT, div_tot TEXT, gun_time BIGINT, hometown TEXT, net_time BIGINT, num BIGINT, pace BIGINT, person_name TEXT, place BIGINT);

-- INSERT INTO marathon_female_EY_46 (age, div_tot, gun_time, hometown, net_time, num, pace, person_name, place) VALUES (28, '1/100', 3650, 'San Francisco', 3600, 201, 510, 'Jane Doe', 1), (26, '2/100', 3900, 'Los Angeles', 3850, 202, 530, 'Emily Davis', 2), (24, '3/100', 4100, 'Seattle', 4050, 203, 590, 'Anna Brown', 3);

WITH u as (
SELECT *, 'male' as 'gender'
FROM marathon_male_EY_46 m
UNION
SELECT *,'female' as 'gender' 
FROM marathon_female_EY_46 f
), g_diff as (
SELECT gender, AVG(gun_time) - AVG(net_time) as 'diff'
FROM u
GROUP BY gender
)
SELECT diff - female as 'absolute' FROM (
	SELECT *,LAG(diff,1) OVER (ORDER BY diff) as 'female'
	FROM g_diff
) L
WHERE female <> 0

from pyspark.sql import Window
from pyspark.sql.functions import col, lit, avg, abs, lag

m = marathon_male_EY_46.withColumn("gender",lit("male"))
f = marathon_female_EY_46.withColumn("gender",lit("female"))

u = m.union(f)

g_diff = u.agg(AVG("gun_time")-AVG("net_time").alias("diff"))\
	.groupBy("gender")
    
window_spec = Window.orderBy(col("diff"))

diffff = g_diff.withColumn("female",lag("diff",1).over(window_spec))\
	.withColumn("absolute",ABS("diff" - "female"))
	.select("absolute").show()
    .filter(`col("prev_diff").isna())  

