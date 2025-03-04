-- CREATE TABLE sessions_walmart_40(session_id INT, user_id INT, session_date DATETIME);

-- INSERT INTO sessions_walmart_40 (session_id, user_id, session_date) VALUES (1, 1, '2024-01-01'), (2, 2, '2024-01-02'), (3, 3, '2024-01-05'), (4, 3, '2024-01-05'), (5, 4, '2024-01-03'), (6, 4, '2024-01-03'), (7, 5, '2024-01-04'), 
-- (8, 5, '2024-01-04'), (9, 3, '2024-01-05'), (10, 5, '2024-01-04');

-- CREATE TABLE order_summary_walmart_40 (order_id INT, user_id INT, order_value INT, order_date DATETIME);

-- INSERT INTO order_summary_walmart_40 (order_id, user_id, order_value, order_date) VALUES (1, 1, 152, '2024-01-01'), (2, 2, 485, '2024-01-02'), (3, 3, 398, '2024-01-05'), (4, 3, 320, '2024-01-05'),
--  (5, 4, 156, '2024-01-03'), (6, 4, 121, '2024-01-03'), (7, 5, 238, '2024-01-04'), (8, 5, 70, '2024-01-04'), (9, 3, 152, '2024-01-05'), (10, 5, 171, '2024-01-04');

SELECT DISTINCT s.user_id,session_date,COUNT(*) OVER(PARTITION BY s.user_id) as 'count_order',SUM(order_value) OVER(PARTITION BY o.user_id) as 'total_value'
FROM sessions_walmart_40 s
JOIN order_summary_walmart_40 o
ON s.user_id = o.user_id
WHERE session_date = order_date


window_spec = Window.partitionBy('user_id')

j = sessions_walmart_40.join(order_summary_walmart_40,on = 'user_id',how = 'inner')

j.withColumn('count_order',count('user_id').over(window_spec))\
	.withColumn('total_value',sum('order_value').over(window_spec))
    .filter(col('session_date') == col('order_date'))
    .select(sessions_walmart_40.col('user_id'),'session_date','count_order','total_value').distinct()\
    .show()