-- ============================================================
-- Project  : Scale Model Car Store — Advanced Analytics
-- Database : stores.db  (SQLite)
-- Author   : Jurgen B.
-- Phase    : 2 — Employee Performance, Revenue Trends &
--                Customer Segmentation
-- ============================================================


-- ============================================================
-- ANALYSIS 1: Sales Rep & Manager Performance
-- Business question: Which sales reps and offices generate
-- the most profit, and how does the team hierarchy look?
-- ============================================================

-- 1a. Sales Rep Performance
-- Joins employees to customers, orders, orderdetails and products
-- to compute total customers, orders and profit per rep.

SELECT e.employeeNumber,
       e.firstName || ' ' || e.lastName               AS sales_rep,
       e.jobTitle,
       o.city                                          AS office,
       COUNT(DISTINCT c.customerNumber)                AS total_customers,
       COUNT(DISTINCT ord.orderNumber)                 AS total_orders,
       ROUND(SUM(od.quantityOrdered * (od.priceEach - p.buyPrice)), 2) AS total_profit
  FROM employees    e
  JOIN offices      o   ON o.officeCode              = e.officeCode
  JOIN customers    c   ON c.salesRepEmployeeNumber  = e.employeeNumber
  JOIN orders       ord ON ord.customerNumber        = c.customerNumber
  JOIN orderdetails od  ON od.orderNumber            = ord.orderNumber
  JOIN products     p   ON p.productCode             = od.productCode
 GROUP BY e.employeeNumber
 ORDER BY total_profit DESC;


-- 1b. Management Hierarchy (Self-Join)
-- Uses a self-join on the employees table to map each employee
-- to their manager via the reportsTo column.

SELECT e.firstName || ' ' || e.lastName     AS employee,
       e.jobTitle,
       m.firstName || ' ' || m.lastName     AS reports_to,
       m.jobTitle                           AS manager_title,
       o.city                               AS office
  FROM employees e
  LEFT JOIN employees m ON m.employeeNumber = e.reportsTo
  JOIN offices o ON o.officeCode = e.officeCode
 ORDER BY m.lastName, e.lastName;


-- 1c. Office Performance
-- Aggregates profit, customers and reps at the office level
-- to identify which locations drive the most value.

SELECT o.city,
       o.country,
       o.territory,
       COUNT(DISTINCT e.employeeNumber)               AS num_reps,
       COUNT(DISTINCT c.customerNumber)               AS num_customers,
       ROUND(SUM(od.quantityOrdered * (od.priceEach - p.buyPrice)), 2) AS total_profit
  FROM offices      o
  JOIN employees    e   ON e.officeCode             = o.officeCode
  JOIN customers    c   ON c.salesRepEmployeeNumber = e.employeeNumber
  JOIN orders       ord ON ord.customerNumber       = c.customerNumber
  JOIN orderdetails od  ON od.orderNumber           = ord.orderNumber
  JOIN products     p   ON p.productCode            = od.productCode
 GROUP BY o.officeCode
 ORDER BY total_profit DESC;


-- ============================================================
-- ANALYSIS 2: Monthly Revenue Trends
-- Business question: How has revenue and profit changed over
-- time, and which product lines drive seasonal peaks?
-- ============================================================

-- 2a. Monthly Revenue & Profit
-- Uses strftime() to extract year and month from order dates,
-- then aggregates revenue and profit per period.

SELECT strftime('%Y', ord.orderDate)                               AS year,
       strftime('%m', ord.orderDate)                               AS month,
       COUNT(DISTINCT ord.orderNumber)                             AS num_orders,
       ROUND(SUM(od.quantityOrdered * od.priceEach), 2)            AS revenue,
       ROUND(SUM(od.quantityOrdered * (od.priceEach - p.buyPrice)), 2) AS profit
  FROM orders       ord
  JOIN orderdetails od ON od.orderNumber = ord.orderNumber
  JOIN products     p  ON p.productCode  = od.productCode
 WHERE ord.status != 'Cancelled'
 GROUP BY year, month
 ORDER BY year, month;


-- 2b. Revenue by Product Line per Year
-- Breaks down annual performance by product line to identify
-- which categories grow or decline year on year.

SELECT strftime('%Y', ord.orderDate)                               AS year,
       p.productLine,
       ROUND(SUM(od.quantityOrdered * od.priceEach), 2)            AS revenue,
       ROUND(SUM(od.quantityOrdered * (od.priceEach - p.buyPrice)), 2) AS profit
  FROM orders       ord
  JOIN orderdetails od ON od.orderNumber = ord.orderNumber
  JOIN products     p  ON p.productCode  = od.productCode
 WHERE ord.status != 'Cancelled'
 GROUP BY year, p.productLine
 ORDER BY year, profit DESC;


-- 2c. Order Fulfilment Time
-- Uses julianday() to calculate the number of days between
-- order date and ship date, broken down by order status.

SELECT ord.status,
       COUNT(*)                                                            AS num_orders,
       ROUND(AVG(julianday(ord.shippedDate) - julianday(ord.orderDate)), 1) AS avg_days_to_ship,
       MIN(julianday(ord.shippedDate) - julianday(ord.orderDate))           AS min_days,
       MAX(julianday(ord.shippedDate) - julianday(ord.orderDate))           AS max_days
  FROM orders ord
 WHERE ord.shippedDate IS NOT NULL
 GROUP BY ord.status;


-- ============================================================
-- ANALYSIS 3: Customer Segmentation
-- Business question: How do we categorise customers by value,
-- and who is at risk of churning?
-- ============================================================

-- 3a. Customer Segmentation (CASE WHEN)
-- Assigns each customer to a tier based on total profit
-- generated: Platinum / Gold / Silver / Bronze.
--
-- Thresholds:
--   Platinum : profit >= $100,000
--   Gold     : profit >= $50,000
--   Silver   : profit >= $20,000
--   Bronze   : profit <  $20,000

WITH customer_profit AS (
    SELECT c.customerNumber,
           c.customerName,
           c.city,
           c.country,
           ROUND(SUM(od.quantityOrdered * (od.priceEach - p.buyPrice)), 2) AS total_profit,
           COUNT(DISTINCT ord.orderNumber)                                  AS total_orders
      FROM customers    c
      JOIN orders       ord ON ord.customerNumber = c.customerNumber
      JOIN orderdetails od  ON od.orderNumber     = ord.orderNumber
      JOIN products     p   ON p.productCode      = od.productCode
     GROUP BY c.customerNumber
)
SELECT customerNumber,
       customerName,
       city,
       country,
       total_profit,
       total_orders,
       CASE
           WHEN total_profit >= 100000 THEN 'Platinum'
           WHEN total_profit >=  50000 THEN 'Gold'
           WHEN total_profit >=  20000 THEN 'Silver'
           ELSE                             'Bronze'
       END AS segment
  FROM customer_profit
 ORDER BY total_profit DESC;


-- 3b. Segment Summary
-- Summarises the number of customers, average profit and total
-- profit for each segment — useful for resource allocation.

WITH customer_profit AS (
    SELECT c.customerNumber,
           ROUND(SUM(od.quantityOrdered * (od.priceEach - p.buyPrice)), 2) AS total_profit,
           COUNT(DISTINCT ord.orderNumber)                                  AS total_orders
      FROM customers    c
      JOIN orders       ord ON ord.customerNumber = c.customerNumber
      JOIN orderdetails od  ON od.orderNumber     = ord.orderNumber
      JOIN products     p   ON p.productCode      = od.productCode
     GROUP BY c.customerNumber
),
segmented AS (
    SELECT CASE
               WHEN total_profit >= 100000 THEN 'Platinum'
               WHEN total_profit >=  50000 THEN 'Gold'
               WHEN total_profit >=  20000 THEN 'Silver'
               ELSE                             'Bronze'
           END AS segment,
           total_profit,
           total_orders
      FROM customer_profit
)
SELECT segment,
       COUNT(*)                    AS num_customers,
       ROUND(AVG(total_profit), 2) AS avg_profit,
       ROUND(SUM(total_profit), 2) AS total_profit,
       ROUND(AVG(total_orders), 1) AS avg_orders
  FROM segmented
 GROUP BY segment
 ORDER BY total_profit DESC;


-- 3c. Dormant Customer Identification (Churn Risk)
-- Finds customers whose last order was more than 180 days
-- before the end of the dataset (June 2005).
-- Uses julianday() and MAX(orderDate) per customer.

SELECT c.customerNumber,
       c.customerName,
       c.city,
       c.country,
       MAX(ord.orderDate)                                              AS last_order_date,
       ROUND(julianday('2005-06-01') - julianday(MAX(ord.orderDate))) AS days_since_order
  FROM customers c
  JOIN orders ord ON ord.customerNumber = c.customerNumber
 GROUP BY c.customerNumber
HAVING days_since_order > 180
 ORDER BY days_since_order DESC;

-- ============================================================
-- End of Phase 2 analysis
-- ============================================================
