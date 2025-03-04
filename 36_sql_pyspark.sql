-- CREATE TABLE signups_uber_36 (signup_id INT PRIMARY KEY, signup_start_date DATETIME, signup_stop_date DATETIME, plan_id INT, location VARCHAR(100));

-- INSERT INTO signups_uber_36 (signup_id, signup_start_date, signup_stop_date, plan_id, location) VALUES (1, '2020-01-01 10:00:00', '2020-01-01 12:00:00', 101, 'New York'), (2, '2020-01-02 11:00:00', '2020-01-02 13:00:00', 102, 'Los Angeles'), 
-- (3, '2020-01-03 10:00:00', '2020-01-03 14:00:00', 103, 'Chicago'), (4, '2020-01-04 09:00:00', '2020-01-04 10:30:00', 101, 'San Francisco'), (5, '2020-01-05 08:00:00', '2020-01-05 11:00:00', 102, 'New York');

-- CREATE TABLE transactions_uber_36 (transaction_id INT PRIMARY KEY,signup_id INT,transaction_start_date DATETIME,amt FLOAT);

-- INSERT INTO transactions_uber_36 (transaction_id, signup_id, transaction_start_date, amt) VALUES (1, 1, '2020-01-01 10:30:00', 50.00), (2, 1, '2020-01-01 11:00:00', 30.00), (3, 2, '2020-01-02 11:30:00', 100.00), (4, 2, '2020-01-02 12:00:00', 75.00),
--  (5, 3, '2020-01-03 10:30:00', 120.00), (6, 4, '2020-01-04 09:15:00', 80.00), (7, 5, '2020-01-05 08:30:00', 90.00);

WITH avg_calc as (
SELECT location,ROUND(AVG(time_to_sec(timediff(signup_stop_date,signup_start_date))/60) OVER(PARTITION BY location),2) as 'avg_duration', ROUND(AVG(amt) OVER(PARTITION BY location),2) as 'avg_amt'
FROM signups_uber_36 s
JOIN transactions_uber_36 t
ON s.signup_id = t.signup_id
)
SELECT DISTINCT *,ROUND(avg_amt/avg_duration,2) as 'ratio'
FROM avg_calc
ORDER BY ratio desc;


-- window_spec = Window.partitionBy('location')

-- cte = signups_uber_36.join(
-- 		transactions_uber_36,
--         signups_uber_36.signup_id == transactions_uber_36.signup_id,
--         'inner')\
-- 	.withColumn(
-- 		'avg_duration',avg(unix_timestamp(signup_stop_date) - unix_timestamp(signup_start_date))/60).over(window_spec))\
--     .withColumn(
-- 		'avg_amt',round(avg(col('amt')).over(window_spec),2))
--     
-- cte.withColumn('ratio',col('avg_duration')/col('avg_amt'))\
-- 	.select('location','avg_duration','avg_amt','ratio')\
--     .show()
