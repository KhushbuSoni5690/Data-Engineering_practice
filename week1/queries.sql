-- Show five rows from the cleaned orders.
SELECT *
FROM 'data/orders_clean.parquet'
LIMIT 5;

-- Calculate the total and average order amount for each city.
SELECT city,
       ROUND(SUM(amount), 2) AS total_amount,
       ROUND(AVG(amount), 2) AS average_amount
FROM 'data/orders_clean.parquet'
GROUP BY city;

-- Find the top three customers by total spend in each city.
WITH customer_totals AS (
    SELECT city,
           customer_name,
           ROUND(SUM(amount), 2) AS total_spend
    FROM 'data/orders_clean.parquet'
    GROUP BY city, customer_name
),
ranked_customers AS (
    SELECT city,
           customer_name,
           total_spend,
           ROW_NUMBER() OVER (
               PARTITION BY city
               ORDER BY total_spend DESC, customer_name
           ) AS customer_rank
    FROM customer_totals
)
SELECT *
FROM ranked_customers
WHERE customer_rank <= 3
ORDER BY city, customer_rank;

-- monthly revenue

with monthly_totals as (
    select DATE_TRUNC('month', order_date) as order_month,
    ROUND(SUM(amount), 2) AS monthly_revenue 
    from 'data/orders_clean.parquet' 
    group by order_month 

)
SELECT order_month,
       monthly_revenue,
       ROUND(
       SUM(monthly_revenue) OVER (
       ORDER BY order_month
       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),
    2
) AS running_total
 from monthly_totals
order by order_month;

-- each customers consequtive orders with lag

