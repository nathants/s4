SELECT passenger_count, count(*) as cnt
FROM yellow_orc
GROUP BY passenger_count
ORDER BY cnt desc;
