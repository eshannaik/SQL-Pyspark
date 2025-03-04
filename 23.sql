-- CREATE TABLE employee_o_Oracle_23 (id INT PRIMARY KEY,first_name VARCHAR(50),last_name VARCHAR(50),age INT,gender VARCHAR(10),employee_title VARCHAR(50),department VARCHAR(50),salary INT,manager_id INT);

-- INSERT INTO employee_o_Oracle_23 (id, first_name, last_name, age, gender, employee_title, department, salary, manager_id) VALUES(1, 'Alice', 'Smith', 45, 'F', 'Manager', 'HR', 9000, 1),(2, 'Bob', 'Johnson', 34, 'M', 'Assistant', 'HR', 4500, 1),
-- (3, 'Charlie', 'Williams', 28, 'M', 'Coordinator', 'HR', 4800, 1),(4, 'Diana', 'Brown', 32, 'F', 'Manager', 'IT', 12000, 4),(5, 'Eve', 'Jones', 27, 'F', 'Analyst', 'IT', 7000, 4),(6, 'Frank', 'Garcia', 29, 'M', 'Developer', 'IT', 7500, 4),
-- (7, 'Grace', 'Miller', 30, 'F', 'Manager', 'Finance', 10000, 7),(8, 'Hank', 'Davis', 26, 'M', 'Analyst', 'Finance', 6200, 7),(9, 'Ivy', 'Martinez', 31, 'F', 'Clerk', 'Finance', 5900, 7),(10, 'John', 'Lopez', 36, 'M', 'Manager', 'Marketing', 11000, 10),
-- (11, 'Kim', 'Gonzales', 29, 'F', 'Specialist', 'Marketing', 6800, 10),(12, 'Leo', 'Wilson', 27, 'M', 'Coordinator', 'Marketing', 6600, 10);

WITH cte as (
	SELECT DISTINCT department,manager_id,ROUND(AVG(salary) OVER(PARTITION BY department),0) as 'average_department_salary'
	FROM employee_o_Oracle_23 
    WHERE employee_title <> 'Manager'
)
SELECT e1.department,e1.id,e1.salary,CASE WHEN e1.id=e1.manager_id THEN NULL ELSE e2.salary END 'manager_salary',c.average_department_salary
FROM employee_o_Oracle_23 e1
JOIN employee_o_Oracle_23 e2
ON e1.manager_id = e2.id
JOIN cte c
ON e1.department = c.department and e2.manager_id = c.manager_id

