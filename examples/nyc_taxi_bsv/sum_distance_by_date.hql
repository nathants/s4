SELECT YEAR(pickup), MONTH(pickup), bigint(sum(distance)) as dst
FROM taxi
GROUP BY YEAR(pickup), MONTH(pickup)
ORDER BY dst desc
LIMIT 9;
