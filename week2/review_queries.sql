-- B4: Top two customers by known spend in each category; done orders only.
WITH customer_totals AS (
    SELECT category, customer_id,
           SUM(CAST(amount AS DECIMAL(18, 2))) AS total_spend
    FROM orders_view
    WHERE status = 'done'
    GROUP BY category, customer_id
), ranked_customers AS (
    SELECT category, customer_id, total_spend,
           ROW_NUMBER() OVER (
               PARTITION BY category
               ORDER BY total_spend DESC NULLS LAST, customer_id
           ) AS customer_rank
    FROM customer_totals
)
SELECT category, customer_id, total_spend, customer_rank
FROM ranked_customers
WHERE customer_rank <= 2
ORDER BY category, customer_rank;
