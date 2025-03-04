-- CREATE TABLE sessions_walmart_19 (session_id INT PRIMARY KEY,user_id INT,session_date DATETIME);

-- INSERT INTO sessions_walmart_19 (session_id, user_id, session_date) VALUES (1, 1, '2024-01-01 00:00:00'),(2, 2, '2024-01-02 00:00:00'),(3, 3, '2024-01-05 00:00:00'),(4, 3, '2024-01-05 00:00:00'),(5, 4, '2024-01-03 00:00:00'),
-- (6, 4, '2024-01-03 00:00:00'),(7, 5, '2024-01-04 00:00:00'),(8, 5, '2024-01-04 00:00:00'),(9, 3, '2024-01-05 00:00:00'),(10, 5, '2024-01-04 00:00:00');

-- CREATE TABLE order_summary_walmart_19 (order_id INT PRIMARY KEY,user_id INT,order_value INT,order_date DATETIME);

-- INSERT INTO order_summary_walmart_19 (order_id, user_id, order_value, order_date) VALUES (1, 1, 152, '2024-01-01 00:00:00'),(2, 2, 485, '2024-01-02 00:00:00'),(3, 3, 398, '2024-01-05 00:00:00'),(4, 3, 320, '2024-01-05 00:00:00'),(5, 4, 156, '2024-01-03 00:00:00'),
-- (6, 4, 121, '2024-01-03 00:00:00'),(7, 5, 238, '2024-01-04 00:00:00'),(8, 5, 70, '2024-01-04 00:00:00'),(9, 3, 152, '2024-01-05 00:00:00'),(10, 5, 171, '2024-01-04 00:00:00');


SELECT * FROM sessions_walmart_19;
SELECT * FROM order_summary_walmart_19;

SELECT DISTINCT s.user_id,session_date,COUNT(order_id) OVER(PARTITION BY s.user_id,session_date) as 'total number of orders', SUM(order_value) OVER(PARTITION BY s.user_id,session_date) as 'total order value'
FROM sessions_walmart_19 s
JOIN order_summary_walmart_19 o
ON s.user_id = o.user_id
WHERE s.session_date = o.order_date
