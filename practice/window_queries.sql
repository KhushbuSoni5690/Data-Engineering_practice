-- C1.1: Rank categories by known revenue within each month; done orders only.
WITH monthly_category_totals AS (
    SELECT DATE_TRUNC('month', order_date) AS order_month, category,
           SUM(CAST(amount AS DECIMAL(18, 2))) AS category_revenue
    FROM orders_view
    WHERE status = 'done'
    GROUP BY DATE_TRUNC('month', order_date), category
)
SELECT order_month, category, category_revenue,
       RANK() OVER (
           PARTITION BY order_month
           ORDER BY category_revenue DESC NULLS LAST
       ) AS category_rank
FROM monthly_category_totals
ORDER BY order_month, category_rank, category;

-- C1.2: Show each customer's first dated order, across all statuses.
WITH numbered_orders AS (
    SELECT customer_id, order_id, order_date, amount,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY order_date, order_id
           ) AS order_position
    FROM orders_view
    WHERE order_date IS NOT NULL
)
SELECT customer_id, order_id, order_date, amount
FROM numbered_orders
WHERE order_position = 1
ORDER BY customer_id;

-- C1.3: Each done order's percentage of that customer's known done-order spend.
WITH spending AS (
    SELECT customer_id, order_id, amount,
           SUM(CAST(amount AS DECIMAL(18, 2))) OVER (
               PARTITION BY customer_id
           ) AS customer_total
    FROM orders_view
    WHERE status = 'done'
)
SELECT customer_id, order_id, amount, customer_total,
       CASE WHEN customer_total IS NULL OR customer_total = 0 THEN NULL
            ELSE ROUND(100.0 * amount / customer_total, 2)
       END AS percentage_of_customer_spend
FROM spending
ORDER BY customer_id, order_id;
