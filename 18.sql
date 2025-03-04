-- CREATE TABLE worker_amazon_18(worker_id INT PRIMARY KEY,first_name VARCHAR(50),last_name VARCHAR(50),salary INT,joining_date DATETIME,department VARCHAR(50));

-- INSERT INTO worker_amazon_18(worker_id, first_name, last_name, salary, joining_date, department) VALUES(1, 'John', 'Doe', 80000, '2020-01-15', 'Engineering'),(2, 'Jane', 'Smith', 120000, '2019-03-10', 'Marketing'),(3, 'Alice', 'Brown', 120000, '2021-06-21', 'Sales'),(4, 'Bob', 'Davis', 75000, '2018-04-30', 'Engineering'),(5, 'Charlie', 'Miller', 95000, '2021-01-15', 'Sales');

-- CREATE TABLE title_amazon_18(worker_ref_id INT,worker_title VARCHAR(50),affected_from DATETIME);

-- INSERT INTO title_amazon_18(worker_ref_id, worker_title, affected_from) VALUES(1, 'Engineer', '2020-01-15'),(2, 'Marketing Manager', '2019-03-10'),(3, 'Sales Manager', '2021-06-21'),
-- (4, 'Junior Engineer', '2018-04-30'),(5, 'Senior Salesperson', '2021-01-15');

SELECT * FROM worker_amazon_18;
SELECT * FROM title_amazon_18;

SELECT worker_title FROM (
SELECT worker_title,RANK() OVER(ORDER BY salary desc) as 'rn'
FROM worker_amazon_18 w
JOIN title_amazon_18 t
ON w.worker_id = t.worker_ref_id
) L
where rn = '1'

