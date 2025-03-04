-- CREATE TABLE ms_user_dimension_microsoft_21 (user_id INT PRIMARY KEY,acc_id INT);
-- INSERT INTO ms_user_dimension_microsoft_21 (user_id, acc_id) VALUES (1, 101),(2, 102),(3, 103),(4, 104),(5, 105);

-- CREATE TABLE ms_acc_dimension_microsoft_21 (acc_id INT PRIMARY KEY,paying_customer VARCHAR(10));
-- INSERT INTO ms_acc_dimension_microsoft_21 (acc_id, paying_customer) VALUES (101, 'Yes'),(102, 'No'),(103, 'Yes'),(104, 'No'),(105, 'No');

-- CREATE TABLE ms_download_facts_microsoft_21 (date DATETIME,user_id INT,downloads INT);
-- INSERT INTO ms_download_facts_microsoft_21 (date, user_id, downloads) VALUES ('2024-10-01', 1, 10),('2024-10-01', 2, 15),('2024-10-02', 1, 8),('2024-10-02', 3, 12),('2024-10-02', 4, 20),('2024-10-03', 2, 25),('2024-10-03', 5, 18);

WITH cte as (
SELECT 
	DISTINCT 
		date,
        SUM(CASE WHEN paying_customer = 'No' THEN downloads ELSE NULL END) OVER( PARTITION BY paying_customer,date) as 'non_paying',
		SUM(CASE WHEN paying_customer = 'Yes' THEN downloads ELSE NULL END) OVER( PARTITION BY paying_customer,date) as 'paying'
FROM ms_user_dimension_microsoft_21 u
JOIN ms_acc_dimension_microsoft_21 a
ON u.acc_id = a.acc_id
JOIN ms_download_facts_microsoft_21 d
ON u.user_id = d.user_id
) 
SELECT date,non_paying,lead_no as 'paying' FROM (
	SELECT *,LEAD(paying,1,0) OVER (PARTITION BY date) as 'lead_no'
	FROM cte
	ORDER BY date asc
) L
WHERE non_paying > lead_no 