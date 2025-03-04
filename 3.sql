-- CREATE TABLE users_google_3(user_id INT, user_name varchar(30));
-- INSERT INTO users_google_3 VALUES (1, 'Karl'), (2, 'Hans'), (3, 'Emma'), (4, 'Emma'), (5, 'Mike'), (6, 'Lucas'), (7, 'Sarah'), (8, 'Lucas'), (9, 'Anna'), (10, 'John');

-- CREATE TABLE friends_google_3(user_id INT, friend_id INT);
-- INSERT INTO friends_google_3 VALUES (1,3),(1,5),(2,3),(2,4),(3,1),(3,2),(3,6),(4,7),(5,8),(6,9),(7,10),(8,6),(9,10),(10,7),(10,9);

SELECT * FROM users_google_3;
SELECT * FROM friends_google_3;

SELECT DISTINCT u.user_id,user_name
FROM users_google_3 u
JOIN friends_google_3 f
ON u.user_id = f.user_id
WHERE friend_id = 1 OR friend_id = 2