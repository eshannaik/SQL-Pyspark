-- CREATE TABLE sf_events_netflix_37 (date DATETIME,account_id VARCHAR(10),user_id VARCHAR(10));

-- INSERT INTO sf_events_netflix_37 (date, account_id, user_id) VALUES('2021-01-01', 'A1', 'U1'),('2021-01-01', 'A1', 'U2'),('2021-01-06', 'A1', 'U3'),('2021-01-02', 'A1', 'U1'),('2020-12-24', 'A1', 'U2'),('2020-12-08', 'A1', 'U1'),
-- ('2020-12-09', 'A1', 'U1'),('2021-01-10', 'A2', 'U4'),('2021-01-11', 'A2', 'U4'),('2021-01-12', 'A2', 'U4'),('2021-01-15', 'A2', 'U5'),('2020-12-17', 'A2', 'U4'),('2020-12-25', 'A3', 'U6'),('2020-12-25', 'A3', 'U6'),
-- ('2020-12-25', 'A3', 'U6'),('2020-12-06', 'A3', 'U7'),('2020-12-06', 'A3', 'U6'),('2021-01-14', 'A3', 'U6'),('2021-02-07', 'A1', 'U1'),('2021-02-10', 'A1', 'U2'),('2021-02-01', 'A2', 'U4'),('2021-02-01', 'A2', 'U5'),('2020-12-05', 'A1', 'U8');

SELECT user_id FROM (
	SELECT user_id,day1,day2,date
	FROM (
		SELECT user_id,date,LEAD(date,1,0) OVER(PARTITION BY user_id ORDER BY date asc) as 'day1' ,LEAD(date,2,0) OVER(PARTITION BY user_id ORDER BY date asc) as 'day2'
		FROM sf_events_netflix_37 
	) L
	HAVING DATEDIFF(day1,date) = 1 AND DATEDIFF(day2,date) = 2
)X;

-- window_spec = Window.partitionBy(user_id).orderBy('date',ascending=False)

-- sf_events_netflix_37.withColumn('day1',LEAD('date',1).over(window_spec))\
-- 	.withColumn('day2',LEAD('date',2).over(window_spec))\
-- 	.filter((unix_timestamp(col('day1')) - unix_date(col('date')) == 86400) & (unix_timestamp(col('day2')) - unix_date(col('date')) == 172800))\
-- 	.select('user_id').distinct()\
--     .show()

