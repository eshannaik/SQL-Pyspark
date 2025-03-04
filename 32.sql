-- CREATE TABLE marketing_campaign_accenture_32 (user_id INT,created_at DATETIME,product_id INT,quantity INT,price INT);

-- INSERT INTO marketing_campaign_accenture_32 (user_id, created_at, product_id, quantity, price) VALUES(1, '2020-01-01', 101, 25, 200),(2, '2020-01-01', 102, 5, 150),(3, '2020-01-02', 103, 15, 300),(4, '2020-01-03', 101, 10, 200),
-- (5, '2020-01-04', 102, 22, 150),(6, '2020-01-05', 104, 8, 120),(7, '2020-01-06', 105, 18, 250),(8, '2020-01-07', 101, 30, 200),(9, '2020-01-08', 103, 20, 300),(10, '2020-01-09', 104, 9, 120);

SELECT DISTINCT 
	product_id,c,
	CASE
		WHEN c >= 30 THEN 'Outstanding'
		WHEN c BETWEEN 20 AND 29 THEN 'Satisfactory'
		WHEN c BETWEEN 10 AND 19 THEN 'Unsatisfactory'
		WHEN c BETWEEN 1 AND 9 THEN 'Poor'
	END as 'Clasification' 
FROM (
SELECT *,SUM(quantity) OVER(PARTITION BY product_id) as 'c'
FROM marketing_campaign_accenture_32
) L
ORDER BY c desc