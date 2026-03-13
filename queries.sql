SELECT EXTRACT(MONTH FROM created_at) AS month, 
SUM(amount) AS total 
FROM transactions 
GROUP BY month 
ORDER BY total DESC 
LIMIT 3;

