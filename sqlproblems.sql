-- sqlproblems.sql

-- 1Find the average order value per user, but only show users whose average is above 1000.
-- 2Find the top 3 categories by average transaction amount, not total.
-- 3Find the total revenue per month, but only for months where total revenue is above 1.3 billion.

SELECT user_id, ROUND(AVG(amount)::numeric, 2) AS avg_order_value
FROM transactions 
GROUP BY user_id 
HAVING AVG(amount) > 500 -- no alias (avg_order_value) in having (WHERE is an aggregate function)
ORDER BY avg_order_value DESC
LIMIT 10;


SELECT category, avg_order_value as avg_transaction_amount
FROM analytics_category_revenue
ORDER BY avg_transaction_amount DESC
LIMIT 3;

-- Good — and you found a shortcut. Instead of writing the JOIN yourself you queried analytics_category_revenue which already has the data pre-aggregated.
-- That's valid and shows good instinct — use what's already there.
-- But try it once the long way — with a JOIN from transactions and products — so you know how to get there from raw data. That's what you'd need if the analytics table didn't exist.
-- Then do number 3.

-- 2Find the top 3 categories by average transaction amount, not total.


SELECT l.email, c.campaign_name
FROM leads l
LEFT JOIN campaigns c
ON l.campaign_id = c.campaign_id;

SELECT p.category, ROUND(AVG(t.amount)::numeric,2) AS avg_transaction_amount
FROM products p
LEFT JOIN transactions t
ON p.product_id = t.product_id
GROUP BY p.category
ORDER BY avg_transaction_amount DESC
LIMIT 3;


-- 3Find the total revenue per month, but only for months where total revenue is above 1.3 billion.


SELECT EXTRACT(MONTH FROM created_at) AS month, ROUND(SUM(amount)::numeric,2) AS total_revenue
FROM transactions   
HAVING SUM(amount) > 1300000000
GROUP BY month 
ORDER BY AVG(amount) DESC;

