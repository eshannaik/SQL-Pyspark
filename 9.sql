-- CREATE TABLE nominee_information_netflix_9(name varchar(20), amg_person_id varchar(10), top_genre varchar(10), birthday datetime, id int);

-- INSERT INTO nominee_information_netflix_9 VALUES('Jennifer Lawrence','P562566','Drama','1990-08-15',755),('Jonah Hill','P418718','Comedy','1983-12-20',747),('Anne Hathaway', 'P292630','Drama', '1982-11-12',744),
-- ('Jennifer Hudson','P454405','Drama', '1981-09-12',742),('Rinko Kikuchi', 'P475244','Drama', '1981-01-06', 739);

-- CREATE TABLE oscar_nominees_netflix_9(year int, category varchar(30), nominee varchar(20), movie varchar(30), winner int, id int);

-- INSERT INTO oscar_nominees_netflix_9 VALUES(2008,'actress in a leading role','Anne Hathaway','Rachel Getting Married',0,77),(2012,'actress in a supporting role','Anne HathawayLes','Mis_rables',1,78),
-- (2006,'actress in a supporting role','Jennifer Hudson','Dreamgirls',1,711),(2010,'actress in a leading role','Jennifer Lawrence','Winters Bone',1,717),
-- (2012,'actress in a leading role','Jennifer Lawrence','Silver Linings Playbook',1,718),(2011,'actor in a supporting role','Jonah Hill','Moneyball',0,799),
-- (2006,'actress in a supporting role','Rinko Kikuchi','Babel',0,1253);

SELECT *
FROM nominee_information_netflix_9 n
JOIN oscar_nominees_netflix_9 o
ON n.name = o.nominee;

WITH cte as (
	SELECT n.name,top_genre,SUM(winner) OVER(PARTITION BY n.name ORDER BY n.name asc) as 'winners'
	FROM nominee_information_netflix_9 n
	JOIN oscar_nominees_netflix_9 o
	ON n.name = o.nominee
)
SELECT top_genre FROM (
	SELECT name,top_genre,winners,ROW_NUMBER() OVER(ORDER BY winners desc,name asc) as 'rn'
	FROM CTE
) L
WHERE rn = '1'