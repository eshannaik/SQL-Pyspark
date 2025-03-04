-- CREATE TABLE workers_amazon_48 (department VARCHAR(100), first_name VARCHAR(50), joining_date DATE, last_name VARCHAR(50), salary BIGINT, worker_id BIGINT PRIMARY KEY);

-- INSERT INTO workers_amazon_48 (department, first_name, joining_date, last_name, salary, worker_id) VALUES  ('HR', 'Alice', '2020-01-15', 'Smith', 60000, 1), ('Engineering', 'Bob', '2019-03-10', 'Johnson', 80000, 2), 
-- ('Sales', 'Charlie', '2021-07-01', 'Brown', 50000, 3), ('Engineering', 'David', '2018-12-20', 'Wilson', 90000, 4), ('Marketing', 'Emma', '2020-06-30', 'Taylor', 70000, 5);

-- CREATE TABLE titles_amazon_48 ( affected_from DATE, worker_ref_id BIGINT, worker_title VARCHAR(100), FOREIGN KEY (worker_ref_id) REFERENCES workers_amazon_48(worker_id));

-- INSERT INTO titles_amazon_48 (affected_from, worker_ref_id, worker_title) VALUES  ('2020-01-15', 1, 'HR Manager'), ('2019-03-10', 2, 'Software Engineer'), ('2021-07-01', 3, 'Sales Representative'), 
-- ('2018-12-20', 4, 'Engineering Manager'), ('2020-06-30', 5, 'Marketing Specialist'), ('2022-01-01', 5, 'Marketing Manager');

SELECT w.first_name,t.worker_title
FROM workers_amazon_48 w
JOIN titles_amazon_48 t
ON w.worker_id = t.worker_ref_id
WHERE t.worker_title like '%Manager%'


df = workers_amazon_48.join(titles_amazon_48,on="worker_id",how="inner")

df.filter(col("worker_title").like('%Manager%')).select("first_name","worker_title")