-- ==============================================================================
-- E-Commerce Real-Time Data Pipeline — Phase 7: SQL Analytics
--
-- Target Dataset View: `orders` (registered from S3 Parquet)
-- Schema:
--   - order_id        : INT
--   - customer_id     : INT
--   - product_id      : INT
--   - quantity        : INT
--   - amount          : DOUBLE (Unit Price)
--   - order_status    : STRING ('COMPLETED', 'PENDING', 'CANCELLED')
--   - order_timestamp : STRING (ISO format: 'YYYY-MM-DDTHH:MM:SS')
--   - total_value     : DOUBLE (quantity * amount)
-- ==============================================================================


-- ------------------------------------------------------------------------------
-- 1. Total Sales and Total Order Count
-- Purpose: Overall pipeline volume and gross merchandise value (GMV).
-- Concept: Basic aggregate functions SUM() and COUNT().
-- ------------------------------------------------------------------------------
SELECT
    ROUND(SUM(total_value), 2) AS total_sales,
    COUNT(order_id)            AS total_orders
FROM orders;


-- ------------------------------------------------------------------------------
-- 2. Average Order Value (AOV)
-- Purpose: Key e-commerce metric measuring the average dollar amount per order.
-- Concept: AVG() aggregate function on calculated transaction value.
-- ------------------------------------------------------------------------------
SELECT
    ROUND(AVG(total_value), 2) AS average_order_value
FROM orders;


-- ------------------------------------------------------------------------------
-- 3. Sales by Customer
-- Purpose: Evaluate customer purchasing patterns and total spend.
-- Concept: GROUP BY on customer_id with COUNT() and SUM().
-- ------------------------------------------------------------------------------
SELECT
    customer_id,
    COUNT(order_id)            AS order_count,
    ROUND(SUM(total_value), 2) AS total_sales
FROM orders
GROUP BY customer_id
ORDER BY total_sales DESC;


-- ------------------------------------------------------------------------------
-- 4. Top Customers by Total Sales
-- Purpose: Identify high-value customers (VIP segment).
-- Concept: GROUP BY with ORDER BY ... DESC and LIMIT clause.
-- ------------------------------------------------------------------------------
SELECT
    customer_id,
    ROUND(SUM(total_value), 2) AS total_sales
FROM orders
GROUP BY customer_id
ORDER BY total_sales DESC
LIMIT 5;


-- ------------------------------------------------------------------------------
-- 5. Sales by Product
-- Purpose: Product performance analysis (units moved vs. revenue generated).
-- Concept: Aggregating volume (SUM(quantity)) and revenue by product_id.
-- ------------------------------------------------------------------------------
SELECT
    product_id,
    SUM(quantity)              AS total_quantity_sold,
    ROUND(SUM(total_value), 2) AS total_sales
FROM orders
GROUP BY product_id
ORDER BY total_sales DESC;


-- ------------------------------------------------------------------------------
-- 6. Daily Sales Breakdown
-- Purpose: Monitor daily revenue trends and order frequency over time.
-- Concept: Parsing ISO timestamp using TO_DATE() and grouping by date.
-- ------------------------------------------------------------------------------
SELECT
    TO_DATE(order_timestamp)   AS order_date,
    COUNT(order_id)            AS total_orders,
    ROUND(SUM(total_value), 2) AS daily_sales
FROM orders
GROUP BY TO_DATE(order_timestamp)
ORDER BY order_date ASC;


-- ------------------------------------------------------------------------------
-- 7. Monthly Sales Breakdown
-- Purpose: Macro-level revenue tracking and monthly trend reporting.
-- Concept: DATE_FORMAT(TO_TIMESTAMP(...), 'yyyy-MM') to truncate to monthly bucket.
-- ------------------------------------------------------------------------------
SELECT
    DATE_FORMAT(TO_TIMESTAMP(order_timestamp), 'yyyy-MM') AS order_month,
    COUNT(order_id)                                       AS total_orders,
    ROUND(SUM(total_value), 2)                            AS monthly_sales
FROM orders
GROUP BY DATE_FORMAT(TO_TIMESTAMP(order_timestamp), 'yyyy-MM')
ORDER BY order_month ASC;


-- ------------------------------------------------------------------------------
-- 8. Running Total of Sales Over Time
-- Purpose: Track cumulative sales progression across chronological days.
-- Concept: Window function SUM(...) OVER (ORDER BY ... ROWS BETWEEN ...).
-- Interview Tip: 'ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW' ensures
-- deterministic running accumulation.
-- ------------------------------------------------------------------------------
WITH daily_sales_summary AS (
    SELECT
        TO_DATE(order_timestamp)   AS order_date,
        ROUND(SUM(total_value), 2) AS daily_sales
    FROM orders
    GROUP BY TO_DATE(order_timestamp)
)
SELECT
    order_date,
    daily_sales,
    ROUND(
        SUM(daily_sales) OVER (
            ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        2
    ) AS running_total_sales
FROM daily_sales_summary
ORDER BY order_date ASC;


-- ------------------------------------------------------------------------------
-- 9. Month-Over-Month (MoM) Sales Change Using LAG()
-- Purpose: Financial KPI to analyze growth or contraction between consecutive months.
-- Concept: Window function LAG(col, 1) OVER (ORDER BY ...) to fetch previous row value.
-- ------------------------------------------------------------------------------
WITH monthly_sales_summary AS (
    SELECT
        DATE_FORMAT(TO_TIMESTAMP(order_timestamp), 'yyyy-MM') AS order_month,
        ROUND(SUM(total_value), 2)                            AS current_month_sales
    FROM orders
    GROUP BY DATE_FORMAT(TO_TIMESTAMP(order_timestamp), 'yyyy-MM')
)
SELECT
    order_month,
    current_month_sales,
    LAG(current_month_sales, 1) OVER (
        ORDER BY order_month ASC
    ) AS previous_month_sales,
    ROUND(
        current_month_sales - LAG(current_month_sales, 1) OVER (ORDER BY order_month ASC),
        2
    ) AS sales_change
FROM monthly_sales_summary
ORDER BY order_month ASC;


-- ------------------------------------------------------------------------------
-- 10. Top 2 Customers Per Month Using ROW_NUMBER()
-- Purpose: Identify the top spenders for each calendar month.
-- Concept: Partitioned window function ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...).
-- Interview Tip: ROW_NUMBER() assigns a unique rank (1, 2, 3...) per partition;
-- wrapping in a CTE allows filtering with WHERE rank_in_month <= 2.
-- ------------------------------------------------------------------------------
WITH customer_monthly_sales AS (
    SELECT
        DATE_FORMAT(TO_TIMESTAMP(order_timestamp), 'yyyy-MM') AS order_month,
        customer_id,
        ROUND(SUM(total_value), 2)                            AS total_sales
    FROM orders
    GROUP BY
        DATE_FORMAT(TO_TIMESTAMP(order_timestamp), 'yyyy-MM'),
        customer_id
),
ranked_customers AS (
    SELECT
        order_month,
        customer_id,
        total_sales,
        ROW_NUMBER() OVER (
            PARTITION BY order_month
            ORDER BY total_sales DESC
        ) AS rank_in_month
    FROM customer_monthly_sales
)
SELECT
    order_month,
    customer_id,
    total_sales,
    rank_in_month
FROM ranked_customers
WHERE rank_in_month <= 2
ORDER BY order_month ASC, rank_in_month ASC;
