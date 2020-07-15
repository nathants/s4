SELECT YEAR(pickup), MONTH(pickup), count(*) as cnt
FROM taxi
GROUP BY YEAR(pickup), MONTH(pickup)
ORDER BY cnt desc
LIMIT 9;
