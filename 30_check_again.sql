-- CREATE TABLE customer_state_log_amazon_30 (cust_id VARCHAR(10),state INT,timestamp TIME);

-- INSERT INTO customer_state_log_amazon_30 (cust_id, state, timestamp) VALUES('c001', 1, '07:00:00'),('c001', 0, '09:30:00'),('c001', 1, '12:00:00'),('c001', 0, '14:30:00'),('c002', 1, '08:00:00'),('c002', 0, '09:30:00'),('c002', 1, '11:00:00'),
-- ('c002', 0, '12:30:00'),('c002', 1, '15:00:00'),('c002', 0, '16:30:00'),('c003', 1, '09:00:00'),('c003', 0, '10:30:00'),('c004', 1, '10:00:00'),('c004', 0, '10:30:00'),('c004', 1, '14:00:00'),('c004', 0, '15:30:00'),('c005', 1, '10:00:00'),
-- ('c005', 0, '14:30:00'),('c005', 1, '15:30:00'),('c005', 0, '18:30:00');

WITH cte as (
SELECT cust_id,state,CAST(timestamp as DATETIME) as 'session_start',CAST(LEAD(timestamp) OVER(PARTITION BY cust_id ORDER BY cust_id asc,timestamp asc) AS DATETIME) as 'logout_time'
FROM customer_state_log_amazon_30
), time_difference as (
SELECT *,CASE WHEN logout_time IS NOT NULL AND state <> 0 THEN TIMESTAMPDIFF(MINUTE,session_start,logout_time)  ELSE 0 END as 'time_logged_in'
FROM cte
WHERE state <> 0
)
SELECT cust_id,FLOOR(SUM(time_logged_in)/60) as 'Total_Logged_on'
FROM time_difference
group by cust_id
ORDER BY Total_Logged_on desc



