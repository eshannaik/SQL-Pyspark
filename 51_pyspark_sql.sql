-- CREATE TABLE com_worker_amazon_51 ( worker_id BIGINT PRIMARY KEY, department VARCHAR(25), first_name VARCHAR(25), last_name VARCHAR(25), joining_date DATETIME, salary BIGINT);

-- INSERT INTO com_worker_amazon_51 (worker_id, department, first_name, last_name, joining_date, salary) VALUES  (1, 'HR', 'John', 'Doe', '2020-01-15', 50000), (2, 'IT', 'Jane', 'Smith', '2019-03-10', 60000), 
-- (3, 'Finance', 'Emily', 'Jones', '2021-06-20', 75000), (4, 'Sales', 'Michael', 'Brown', '2018-09-05', 60000), (5, 'Marketing', 'Chris', 'Johnson', '2022-04-12', 70000), 
-- (6, 'IT', 'David', 'Wilson', '2020-11-01', 80000), (7, 'Finance', 'Sarah', 'Taylor', '2017-05-25', 45000), (8, 'HR', 'James', 'Anderson', '2023-01-09', 65000), 
-- (9, 'Sales', 'Anna', 'Thomas', '2020-02-18', 55000), (10, 'Marketing', 'Robert', 'Jackson', '2021-07-14', 60000);

SELECT salary FROM (
SELECT *,DENSE_RANK() OVER(ORDER BY salary asc) as 'dr'
FROM com_worker_amazon_51
) L
WHERE dr = '5'

from pyspark.sql import functions as F
from pyspark.sql.functions import Window 
from pyspark.sql.functions import dense_rank

window_spec = Window.orderBy(F.col("salary").asc())

dense_r = com_worker_amazon_51.withColumn("dr",dense_rank().over(window_spec))\
	.select("salary","dr")
    
dense_r.filter(F.col("dr") == 5)\
.select("salary")\
.show()

