-- CREATE TABLE car_launches_tesla_8(year int, company_name varchar(15), product_name varchar(30));

-- INSERT INTO car_launches_tesla_8 VALUES(2019,'Toyota','Avalon'),(2019,'Toyota','Camry'),(2020,'Toyota','Corolla'),(2019,'Honda','Accord'),(2019,'Honda','Passport'),(2019,'Honda','CR-V'),(2020,'Honda','Pilot'),
-- (2019,'Honda','Civic'),(2020,'Chevrolet','Trailblazer'),(2020,'Chevrolet','Trax'),(2019,'Chevrolet','Traverse'),(2020,'Chevrolet','Blazer'),(2019,'Ford','Figo'),(2020,'Ford','Aspire'),(2019,'Ford','Endeavour'),(2020,'Jeep','Wrangler');

with cte1 as (
SELECT company_name,SUM(CASE WHEN year = '2020' THEN 1 ELSE 0 END) OVER(PARTITION BY company_name) as'c1'
FROM car_launches_tesla_8
WHERE year = '2020'
), cte2 as (
SELECT company_name,SUM(CASE WHEN year = '2019' THEN 1 ELSE 0 END) OVER(PARTITION BY company_name) as'c2'
FROM car_launches_tesla_8
WHERE year = '2019'
)
SELECT DISTINCT r1.company_name, CASE WHEN c1-c2 IS NULL THEN c1 ELSE c1-c2 END as 'difference'
FROM cte1 r1
LEFT JOIN cte2 r2
ON r1.company_name = r2.company_name
order by difference desc;

SELECT * FROM car_launches_tesla_8