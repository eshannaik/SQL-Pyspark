-- CREATE TABLE famous_facebook_1 (user_id INT, follower_id INT);

-- INSERT INTO famous_facebook_1 VALUES
-- (1, 2), (1, 3), (2, 4), (5, 1), (5, 3), 
-- (11, 7), (12, 8), (13, 5), (13, 10), 
-- (14, 12), (14, 3), (15, 14), (15, 13);

SELECT DISTINCT user_id,(COUNT(follower_id) OVER(PARTITION BY user_id)/COUNT(*) OVER()) * 100 as 'famous_percentage' 
FROM famous_facebook_1

from pyspark.sql import functions as F
from pyspark.sql.window import Window	

tc = famous_facebook_1.agg(F.count("*")).alias("total_count")\
.select("user_id","total_count")

window_spec = Window.partitionBy(F.col("user_id"))
uc = famous_facebook_1.withColumn("user_count",count(F.col("follower_id").over(window_spec)))\
.select("user_id","user_count")

tc.join(uc,on="user_id",how="inner")\
.withColumn("famous_percentage",(F.col("user_count")/F.col("total_count"))*100)\
.select("user_id","famous_percentage")
.show()
