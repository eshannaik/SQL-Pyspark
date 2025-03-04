-- CREATE TABLE playbook_users_apple_43 (user_id INT PRIMARY KEY,created_at DATETIME,company_id INT,language VARCHAR(50),activated_at DATETIME,state VARCHAR(50));

-- INSERT INTO playbook_users_apple_43 (user_id, created_at, company_id, language, activated_at, state) VALUES
-- (1, '2024-01-01 08:00:00', 101, 'English', '2024-01-05 10:00:00', 'Active'),
-- (2, '2024-01-02 09:00:00', 102, 'Spanish', '2024-01-06 11:00:00', 'Inactive'),
-- (3, '2024-01-03 10:00:00', 103, 'French', '2024-01-07 12:00:00', 'Active'),
-- (4, '2024-01-04 11:00:00', 104, 'English', '2024-01-08 13:00:00', 'Active'),
-- (5, '2024-01-05 12:00:00', 105, 'Spanish', '2024-01-09 14:00:00', 'Inactive');

-- CREATE TABLE playbook_events_apple_43 ( user_id INT, occurred_at DATETIME, event_type VARCHAR(50), event_name VARCHAR(50), location VARCHAR(100), device VARCHAR(50));

-- INSERT INTO playbook_events_apple_43 (user_id, occurred_at, event_type, event_name, location, device) VALUES
-- (1, '2024-01-05 14:00:00', 'Click', 'Login', 'USA', 'MacBook-Pro'),
-- (2, '2024-01-06 15:00:00', 'View', 'Dashboard', 'Spain', 'iPhone 5s'),
-- (3, '2024-01-07 16:00:00', 'Click', 'Logout', 'France', 'iPad-air'),
-- (4, '2024-01-08 17:00:00', 'Purchase', 'Subscription', 'USA', 'Windows-Laptop'), (5, '2024-01-09 18:00:00', 'Click', 'Login', 'Spain', 'Android-Phone');
WITH apple as (
SELECT language,COUNT(*) OVER(PARTITION BY language) as 'a_total'
FROM playbook_users_apple_43 u
JOIN playbook_events_apple_43 e
ON u.user_id = e.user_id
WHERE device in ('MacBook-Pro','iPhone 5s','iPad-air')
), naa as (
SELECT language,COUNT(*) OVER(PARTITION BY language) as 'total'
FROM playbook_users_apple_43 u
JOIN playbook_events_apple_43 e
ON u.user_id = e.user_id
) 
SELECT DISTINCT a.language,a_total as 'apple',total as 'overall'
FROM apple a
JOIN naa n
ON a.language = n.language

-- from pyspark.sql import Window
-- from pyspark.sql.functions import col, count

-- window_spec = Window.partitionBy("language")

-- apple = playbook_events_apple_43.filter(col("device").isin('MacBook-Pro','iPhone 5s','iPad-air'))\
-- 	.withColumn('apple',count("*").over(window_spec))\
--     .select("language","apple")

-- naa = playbook_events_apple_43.withColumn('overall',count("*").over(window_spec))\
--     .select("language","overall")
--     
-- apple.join(naa,on="language","inner")\
-- .select("language","apple","overall").distinct().show()