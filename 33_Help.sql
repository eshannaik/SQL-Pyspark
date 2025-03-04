-- CREATE TABLE google_fit_location_google_33 (user_id VARCHAR(50),session_id INT,step_id INT,day INT,latitude FLOAT,longitude FLOAT,altitude FLOAT);

-- INSERT INTO   (user_id, session_id, step_id, day, latitude, longitude, altitude)VALUES('user_1', 101, 1, 1, 37.7749, -122.4194, 15.0),('user_1', 101, 2, 1, 37.7750, -122.4195, 15.5),
-- ('user_1', 101, 3, 1, 37.7751, -122.4196, 16.0),('user_1', 102, 1, 1, 34.0522, -118.2437, 20.0),('user_1', 102, 2, 1, 34.0523, -118.2438, 20.5),('user_2', 201, 1, 1, 40.7128, -74.0060, 5.0),
-- ('user_2', 201, 2, 1, 40.7129, -74.0061, 5.5),('user_2', 202, 1, 1, 51.5074, -0.1278, 10.0),('user_2', 202, 2, 1, 51.5075, -0.1279, 10.5),('user_3', 301, 1, 1, 48.8566, 2.3522, 25.0),('user_3', 301, 2, 1, 48.8567, 2.3523, 25.5);

WITH Low as (
SELECT *,MIN(step_id) OVER(PARTITION BY user_id) as 'minn'
FROM google_fit_location_google_33
), High as (
SELECT *,MAX(step_id) OVER(PARTITION BY user_id) as 'maxx'
FROM google_fit_location_google_33
)
SELECT DISTINCT l.user_id,AVG(ROUND(111*((h.latitude-l.latitude)*2)+((h.longitude-l.longitude)*2),2)) OVER(PARTITION BY l.user_id) as 'Flat_Surface'
FROM Low l
JOIN high h
ON l.user_id = h.user_id
WHERE (l.step_id = minn AND h.step_id = maxx) AND (l.step_id <> h.step_id)
