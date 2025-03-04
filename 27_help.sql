-- CREATE TABLE train_arrivals_GS_27 (train_id INT, arrival_time DATETIME);

-- INSERT INTO train_arrivals_GS_27 (train_id, arrival_time) VALUES(1, '2024-11-17 08:00'),(2, '2024-11-17 08:05'),(3, '2024-11-17 08:05'),(4, '2024-11-17 08:10'),(5, '2024-11-17 08:10'),(6, '2024-11-17 12:15'),(7, '2024-11-17 12:20'),
-- (8, '2024-11-17 12:25'),(9, '2024-11-17 15:00'),(10, '2024-11-17 15:00'),(11, '2024-11-17 15:00'),(12, '2024-11-17 15:06'),(13, '2024-11-17 20:00'),(14, '2024-11-17 20:10');

-- CREATE TABLE train_departures_GS_27 (train_id INT, departure_time DATETIME);

-- INSERT INTO train_departures_GS_27 (train_id, departure_time) VALUES(1, '2024-11-17 08:15'),(2, '2024-11-17 08:10'),(3, '2024-11-17 08:20'),(4, '2024-11-17 08:25'),(5, '2024-11-17 08:20'),(6, '2024-11-17 13:00'),(7, '2024-11-17 12:25'),
-- (8, '2024-11-17 12:30'),(9, '2024-11-17 15:05'),(10, '2024-11-17 15:10'),(11, '2024-11-17 15:15'),(12, '2024-11-17 15:15'),(13, '2024-11-17 20:15'),(14, '2024-11-17 20:15');

-- SELECT * 
-- FROM train_arrivals_GS_27;
-- SELECT * 
-- FROM train_departures_GS_27;

WITH TRAIN as (
SELECT arrival_time as 'time', 1 as 'eventType'
FROM train_arrivals_GS_27
UNION ALL
SELECT departure_time as 'time', -1 as 'eventType'
FROM train_departures_GS_27
)
SELECT MAX(Platform_needed) AS 'min_platforms' FROM (
SELECT SUM(eventType) OVER ( ORDER BY time asc) as 'Platform_needed'
FROM TRAIN
) L
