-- CREATE TABLE transaction_records_visa_45 (customer_id BIGINT, store_id BIGINT, transaction_amount BIGINT, transaction_date DATETIME, transaction_id BIGINT PRIMARY KEY);

-- INSERT INTO transaction_records_visa_45 (customer_id, store_id, transaction_amount, transaction_date, transaction_id) VALUES (101, 1, 500, '2024-01-01 10:15:00', 10001), (102, 2, 1500, '2024-01-02 12:30:00', 10002), 
-- (103, 1, 700, '2024-01-03 14:00:00', 10003), (104, 3, 1200, '2024-01-04 09:45:00', 10004), (105, 2, 800, '2024-01-05 11:20:00', 10005);

-- CREATE TABLE stores_visa_45 (area_name VARCHAR(20), area_size BIGINT, store_id BIGINT PRIMARY KEY, store_location TEXT, store_open_date DATETIME);

-- INSERT INTO stores_visa_45 (area_name, area_size, store_id, store_location, store_open_date) VALUES ('Downtown', 1000, 1, 'Main Street', '2020-01-01'), ('Uptown', 1500, 2, 'Park Avenue', '2021-06-15'), 
-- ('Midtown', 1200, 3, 'Broadway', '2019-11-20'), ('Suburbs', 2000, 4, 'Elm Street', '2018-08-10');

SELECT DISTINCT area_name,COUNT(customer_id) OVER(PARTITION BY area_name)/area_size as 'customer_density'
FROM transaction_records_visa_45 t
JOIN stores_visa_45 s
ON t.store_id = s.store_id
LIMIT 3;

SELECT area_name,COUNT(customer_id)/area_size as 'customer_density'
FROM transaction_records_visa_45 t
JOIN stores_visa_45 s
ON t.store_id = s.store_id
group by area_name,area_size
LIMIT 3;

transaction_records_visa_45.join(stores_visa_45,on="store_id",how="inner")\
	.groupBy("area_name","area_size")\
    .agg(countDistinct("customer_id").alias("unique_customers"))
	.withColumn("customer_density",(col("unique_customers")/col("area_size")))
	.select("area_name","customer_density")\
    .limit(3).show()
