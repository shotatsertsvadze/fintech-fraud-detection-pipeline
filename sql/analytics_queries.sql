-- Total transactions
SELECT COUNT(*) AS total_transactions
FROM transactions_clean;

-- Total revenue
SELECT SUM(amount) AS total_revenue
FROM transactions_clean;

-- Fraud transactions
SELECT COUNT(*) AS fraud_transactions
FROM transactions_clean
WHERE fraud_flag = 1;

-- Fraud rate
SELECT 
    COUNT(CASE WHEN fraud_flag = 1 THEN 1 END) * 1.0 / COUNT(*) AS fraud_rate
FROM transactions_clean;

-- Revenue by country
SELECT country, SUM(amount) AS revenue
FROM transactions_clean
GROUP BY country
ORDER BY revenue DESC;

-- High risk transactions
SELECT *
FROM transactions_clean
WHERE risk_level = 'high';