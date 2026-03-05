# Phase 2 — Advanced Analytics Notebook

**Database:** `stores.db` (SQLite) &nbsp;|&nbsp; **Author:** Jurgen B.

> This notebook extends the Phase 1 sales strategy analysis with three advanced analytical areas:
> employee & manager performance, monthly revenue trends, and customer segmentation.
> All queries are verified against the live database.

---

## Database Schema — How the Tables Fit Together

`stores.db` contains **8 tables** covering the full commercial operation of a scale model car retailer — products, transactions, customers, staff and offices.

```
                         ┌──────────────┐
                         │  productlines │
                         │  productLine  │
                         └──────┬───────┘
                                │ 1
                                │ productLine
                                ▼ N
┌──────────────┐        ┌──────────────┐        ┌──────────────────┐
│   offices    │        │   products   │        │   orderdetails   │
│  officeCode  │        │  productCode │───────►│  productCode  FK │
└──────┬───────┘        └──────────────┘        │  orderNumber  FK │
       │ 1                                       └────────┬─────────┘
       │ officeCode                                       │ N
       ▼ N                                               │ orderNumber
┌──────────────┐                                         ▼ 1
│  employees   │◄──────────────────────────────  ┌──────────────┐
│ employeeNum  │  salesRepEmployeeNumber FK        │    orders    │
│  reportsTo ─►│  (self-join for org chart)       │  orderNumber │
└──────────────┘                                  │ customerNum  │
                                                  └──────┬───────┘
                                                         │ N
                                                         │ customerNumber
                                                         ▼ 1
                                                  ┌──────────────┐       ┌──────────┐
                                                  │  customers   │──────►│ payments │
                                                  │ customerNum  │ 1   N │customerNum│
                                                  └──────────────┘       └──────────┘
```

### Table Summary

| Table | Key Columns | Rows | Role in Phase 2 |
|---|---|---|---|
| `employees` | `employeeNumber`, `reportsTo`, `officeCode` | 23 | Sales rep performance, org chart (self-join) |
| `offices` | `officeCode`, `city`, `country`, `territory` | 7 | Office-level profit rollup |
| `customers` | `customerNumber`, `salesRepEmployeeNumber` | 122 | Links reps to revenue; segmentation & churn |
| `orders` | `orderNumber`, `customerNumber`, `orderDate`, `status` | 326 | Date-based trend analysis; fulfilment time |
| `orderdetails` | `orderNumber`, `productCode`, `quantityOrdered`, `priceEach` | 2,996 | Revenue and profit calculation |
| `products` | `productCode`, `productLine`, `buyPrice` | 110 | Margin calculation; product line trends |
| `productlines` | `productLine` | 7 | Category grouping for trend analysis |
| `payments` | `customerNumber`, `amount` | 273 | Not used directly in Phase 2 |

### Key Relationships Used in Phase 2

| Analysis | Tables Joined | Join Type |
|---|---|---|
| Sales rep profit | `employees → customers → orders → orderdetails → products` | 5-table INNER JOIN |
| Org chart | `employees → employees` | Self-join (LEFT JOIN) |
| Office performance | Above + `offices` | 5-table INNER JOIN |
| Monthly revenue | `orders → orderdetails → products` | 3-table INNER JOIN |
| Product line trends | Above + `productlines` | 4-table INNER JOIN |
| Customer segmentation | `customers → orders → orderdetails → products` | 4-table INNER JOIN |
| Churn risk | `customers → orders` | 2-table INNER JOIN |

> **Note on profit calculation:** Throughout Phase 2, profit is calculated as `quantityOrdered × (priceEach − buyPrice)`. This uses `priceEach` from `orderdetails` (the actual sale price) and `buyPrice` from `products` (the cost price). No operating cost data is available in the database.

---

## Analysis 1 — Sales Rep & Manager Performance

**Business question:** Which sales reps and offices generate the most profit, and how does the management hierarchy look?

---

### 1a. Sales Rep Performance

**Approach:** Join the `employees` table to `customers`, `orders`, `orderdetails` and `products` to calculate total customers managed, orders handled and profit generated per sales rep.

**Techniques:** 5-table `JOIN`, `COUNT(DISTINCT ...)`, `SUM`, `GROUP BY`, `ORDER BY`

```sql
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
```

**Output:**

| Sales Rep | Office | Customers | Orders | Total Profit |
|---|---|---|---|---|
| Gerard Hernandez | Paris | 7 | 43 | $504,644.71 |
| Leslie Jennings | San Francisco | 6 | 34 | $435,208.35 |
| Pamela Castillo | Paris | 10 | 31 | $340,727.90 |
| Larry Bott | London | 8 | 22 | $290,203.59 |
| Barry Jones | London | 9 | 25 | $276,659.25 |
| George Vanauf | NYC | 8 | 22 | $269,596.09 |
| Loui Bondur | Paris | 6 | 20 | $234,891.07 |
| Peter Marsh | Sydney | 5 | 19 | $230,811.75 |
| Andy Fixter | Sydney | 5 | 19 | $222,207.18 |
| Steve Patterson | Boston | 6 | 18 | $197,879.23 |
| Foon Yue Tseng | NYC | 6 | 17 | $194,839.92 |
| Mami Nishi | Tokyo | 5 | 16 | $181,181.80 |
| Martin Gerard | Paris | 5 | 12 | $156,878.63 |
| Julie Firrelli | Boston | 6 | 14 | $152,119.31 |
| Leslie Thompson | San Francisco | 6 | 14 | $138,031.47 |

> **Finding:** Gerard Hernandez (Paris) is the top performer at $504K profit despite managing only 7 customers — the highest profit-per-customer ratio on the team. Paris overall is the standout office.

---

### 1b. Management Hierarchy (Self-Join)

**Approach:** A **self-join** on the `employees` table maps each employee to their manager using the `reportsTo` column, which references another `employeeNumber` in the same table.

**Techniques:** `LEFT JOIN` on same table (self-join), string concatenation with `||`

```sql
SELECT e.firstName || ' ' || e.lastName     AS employee,
       e.jobTitle,
       m.firstName || ' ' || m.lastName     AS reports_to,
       m.jobTitle                           AS manager_title,
       o.city                               AS office
  FROM employees e
  LEFT JOIN employees m ON m.employeeNumber = e.reportsTo
  JOIN offices o ON o.officeCode = e.officeCode
 ORDER BY m.lastName, e.lastName;
```

**Output:**

| Employee | Job Title | Reports To | Manager Title | Office |
|---|---|---|---|---|
| Diane Murphy | President | — | — | San Francisco |
| Mary Patterson | VP Sales | Diane Murphy | President | San Francisco |
| Jeff Firrelli | VP Marketing | Diane Murphy | President | San Francisco |
| Anthony Bow | Sales Manager (NA) | Mary Patterson | VP Sales | San Francisco |
| Gerard Bondur | Sales Manager (EMEA) | Mary Patterson | VP Sales | Paris |
| William Patterson | Sales Manager (APAC) | Mary Patterson | VP Sales | Sydney |
| Mami Nishi | Sales Rep | Mary Patterson | VP Sales | Tokyo |
| Leslie Jennings | Sales Rep | Anthony Bow | Sales Manager (NA) | San Francisco |
| Leslie Thompson | Sales Rep | Anthony Bow | Sales Manager (NA) | San Francisco |
| Julie Firrelli | Sales Rep | Anthony Bow | Sales Manager (NA) | Boston |
| Steve Patterson | Sales Rep | Anthony Bow | Sales Manager (NA) | Boston |
| Foon Yue Tseng | Sales Rep | Anthony Bow | Sales Manager (NA) | NYC |
| George Vanauf | Sales Rep | Anthony Bow | Sales Manager (NA) | NYC |
| Gerard Hernandez | Sales Rep | Gerard Bondur | Sales Manager (EMEA) | Paris |
| Pamela Castillo | Sales Rep | Gerard Bondur | Sales Manager (EMEA) | Paris |
| Loui Bondur | Sales Rep | Gerard Bondur | Sales Manager (EMEA) | Paris |
| Martin Gerard | Sales Rep | Gerard Bondur | Sales Manager (EMEA) | Paris |
| Larry Bott | Sales Rep | Gerard Bondur | Sales Manager (EMEA) | London |
| Barry Jones | Sales Rep | Gerard Bondur | Sales Manager (EMEA) | London |
| Andy Fixter | Sales Rep | William Patterson | Sales Manager (APAC) | Sydney |
| Peter Marsh | Sales Rep | William Patterson | Sales Manager (APAC) | Sydney |
| Tom King | Sales Rep | William Patterson | Sales Manager (APAC) | Sydney |
| Yoshimi Kato | Sales Rep | Mami Nishi | Sales Rep | Tokyo |

> **Note:** `LEFT JOIN` is used so that the President (Diane Murphy) — who has no manager — still appears in the results with a NULL in the `reports_to` column.

---

### 1c. Office Performance

**Approach:** Roll up profit, headcount and customer counts to the office level to compare regional performance.

**Techniques:** 5-table `JOIN`, `COUNT(DISTINCT ...)`, `GROUP BY`

```sql
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
```

**Output:**

| City | Country | Territory | Reps | Customers | Total Profit |
|---|---|---|---|---|---|
| Paris | France | EMEA | 4 | 28 | $1,237,142.31 |
| San Francisco | USA | NA | 2 | 12 | $573,239.82 |
| London | UK | EMEA | 2 | 17 | $566,862.84 |
| NYC | USA | NA | 2 | 14 | $464,436.01 |
| Sydney | Australia | APAC | 2 | 10 | $453,018.93 |
| Boston | USA | NA | 2 | 12 | $349,998.54 |
| Tokyo | Japan | Japan | 1 | 5 | $181,181.80 |

> **Finding:** Paris generates over **$1.2M** in profit — more than double any other office — driven by 4 reps managing 28 customers. Tokyo is significantly underperforming relative to other offices.

---

## Analysis 2 — Monthly Revenue Trends

**Business question:** How has revenue and profit trended over time, and which product lines are driving growth?

---

### 2a. Monthly Revenue & Profit

**Approach:** Use `strftime()` to extract year and month from order dates and aggregate revenue and profit per period. Cancelled orders are excluded.

**Techniques:** `strftime()`, date filtering with `WHERE`, `COUNT(DISTINCT ...)`, `GROUP BY`

```sql
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
```

**Output (selected months):**

| Year | Month | Orders | Revenue | Profit |
|---|---|---|---|---|
| 2003 | 01 | 5 | $116,692.77 | $45,820.95 |
| 2003 | 09 | 8 | $236,697.85 | $93,855.53 |
| 2003 | 10 | 17 | $470,169.12 | $184,166.36 |
| 2003 | 11 | 29 | $965,061.55 | $385,538.43 |
| 2004 | 11 | 33 | $979,291.98 | $392,370.92 |
| 2005 | 05 | 15 | $441,474.94 | $172,133.80 |

> **Finding:** November is consistently the peak month in both 2003 and 2004, with revenue nearly **8× higher** than January. This strong seasonality suggests a major opportunity to prepare inventory and staffing for Q4.

---

### 2b. Revenue by Product Line per Year

**Approach:** Break down annual revenue and profit by product line to track category performance over time.

**Techniques:** `strftime()`, multi-column `GROUP BY`, `ORDER BY` with multiple keys

```sql
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
```

**Output:**

| Year | Product Line | Revenue | Profit |
|---|---|---|---|
| 2003 | Classic Cars | $1,369,386.78 | $540,176.33 |
| 2003 | Vintage Cars | $607,535.32 | $251,750.18 |
| 2003 | Trucks and Buses | $376,657.12 | $147,415.65 |
| 2003 | Motorcycles | $344,998.74 | $144,307.16 |
| 2004 | Classic Cars | $1,689,307.07 | $673,958.15 |
| 2004 | Vintage Cars | $823,927.95 | $337,219.36 |
| 2004 | Motorcycles | $527,243.84 | $222,485.41 |
| 2005 | Classic Cars | $715,953.54 | $280,523.06 |
| 2005 | Vintage Cars | $323,846.30 | $130,691.89 |

> **Finding:** Classic Cars dominates every year — accounting for roughly 40% of total profit. Trains consistently underperform. The 2005 figures are partial (data ends May 2005) and are on track to exceed 2004 levels.

---

### 2c. Order Fulfilment Time

**Approach:** Use `julianday()` to calculate the difference in days between order date and ship date, grouped by order status.

**Techniques:** `julianday()`, `AVG`, `MIN`, `MAX`, `WHERE` with `IS NOT NULL`

```sql
SELECT ord.status,
       COUNT(*)                                                            AS num_orders,
       ROUND(AVG(julianday(ord.shippedDate) - julianday(ord.orderDate)), 1) AS avg_days_to_ship,
       MIN(julianday(ord.shippedDate) - julianday(ord.orderDate))           AS min_days,
       MAX(julianday(ord.shippedDate) - julianday(ord.orderDate))           AS max_days
  FROM orders ord
 WHERE ord.shippedDate IS NOT NULL
 GROUP BY ord.status;
```

**Output:**

| Status | Orders | Avg Days to Ship | Min | Max |
|---|---|---|---|---|
| Shipped | 303 | 3.8 | 1.0 | 65.0 |
| Resolved | 4 | 3.5 | 2.0 | 5.0 |
| Disputed | 3 | 5.0 | 3.0 | 6.0 |
| Cancelled | 2 | 1.5 | 1.0 | 2.0 |

> **Finding:** Average fulfilment time is under 4 days — healthy performance. However the maximum of 65 days for a shipped order is a significant outlier worth investigating.

---

## Analysis 3 — Customer Segmentation

**Business question:** How do we categorise customers by value, and which customers are at risk of churning?

---

### 3a. Customer Segmentation (CASE WHEN)

**Approach:** Calculate total profit per customer, then assign a tier using `CASE WHEN`.

**Tiers:**
- 🥇 **Platinum** — profit ≥ $100,000
- 🥈 **Gold** — profit ≥ $50,000
- 🥉 **Silver** — profit ≥ $20,000
- **Bronze** — profit < $20,000

**Techniques:** CTE, `CASE WHEN`, `COUNT(DISTINCT ...)`, `SUM`, multi-table `JOIN`

```sql
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
```

**Output (Top 20):**

| Customer | City | Country | Profit | Orders | Segment |
|---|---|---|---|---|---|
| Euro+ Shopping Channel | Madrid | Spain | $326,519.66 | 26 | 🥇 Platinum |
| Mini Gifts Distributors Ltd. | San Rafael | USA | $236,769.39 | 17 | 🥇 Platinum |
| Muscle Machine Inc | NYC | USA | $72,370.09 | 4 | 🥈 Gold |
| Australian Collectors, Co. | Melbourne | Australia | $70,311.07 | 5 | 🥈 Gold |
| La Rochelle Gifts | Nantes | France | $60,875.30 | 4 | 🥈 Gold |
| Dragon Souveniers, Ltd. | Singapore | Singapore | $60,477.38 | 5 | 🥈 Gold |
| AV Stores, Co. | Manchester | UK | $60,095.86 | 3 | 🥈 Gold |
| Down Under Souveniers, Inc | Auckland | New Zealand | $60,013.99 | 5 | 🥈 Gold |
| Land of Toys Inc. | NYC | USA | $58,669.10 | 4 | 🥈 Gold |
| The Sharp Gifts Warehouse | San Jose | USA | $55,931.37 | 4 | 🥈 Gold |
| Corporate Gift Ideas Co. | San Francisco | USA | $55,674.28 | 4 | 🥈 Gold |
| Salzburg Collectables | Salzburg | Austria | $54,724.68 | 4 | 🥈 Gold |
| Anna's Decorations, Ltd | North Sydney | Australia | $54,551.66 | 4 | 🥈 Gold |
| Saveley & Henriot, Co. | Lyon | France | $53,211.19 | 3 | 🥈 Gold |
| Reims Collectables | Reims | France | $52,698.66 | 5 | 🥈 Gold |
| Souveniers And Things Co. | Chatswood | Australia | $52,331.45 | 4 | 🥈 Gold |
| Rovelli Gifts | Bergamo | Italy | $52,309.63 | 3 | 🥈 Gold |
| Kelly's Gift Shop | Auckland | New Zealand | $51,771.50 | 4 | 🥈 Gold |
| Danish Wholesale Imports | Kobenhavn | Denmark | $50,973.68 | 5 | 🥈 Gold |
| Corrida Auto Replicas, Ltd | Madrid | Spain | $49,192.39 | 3 | 🥉 Silver |

---

### 3b. Segment Summary

**Approach:** Extend the segmentation CTE to produce a summary table — how many customers are in each tier and how much do they contribute collectively?

**Techniques:** Chained CTEs, `COUNT`, `AVG`, `SUM`, `GROUP BY`

```sql
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
```

**Output:**

| Segment | Customers | Avg Profit | Total Profit | Avg Orders |
|---|---|---|---|---|
| 🥉 Silver | 62 | $33,102.25 | $2,052,339.38 | 2.8 |
| 🥈 Gold | 17 | $57,470.05 | $976,990.89 | 4.1 |
| 🥇 Platinum | 2 | $281,644.53 | $563,289.05 | 21.5 |
| Bronze | 17 | $13,721.23 | $233,260.93 | 2.3 |

> **Finding:** Just **2 Platinum customers** generate $563K — more than the entire Bronze tier (17 customers, $233K). Silver customers collectively produce the most total profit due to volume (62 customers). Retaining Platinum and Gold customers should be the top priority.

---

### 3c. Dormant Customer Identification (Churn Risk)

**Approach:** Find customers whose last order was more than 180 days before the end of the dataset. These are at risk of churning and should be targeted for re-engagement campaigns.

**Techniques:** `MAX(orderDate)`, `julianday()`, `HAVING`, `GROUP BY`

```sql
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
```

**Output (Top 15):**

| Customer | City | Country | Last Order | Days Since Order |
|---|---|---|---|---|
| King Kong Collectables, Co. | Hong Kong | Hong Kong | 2003-12-01 | 548 |
| Men 'R' US Retailers, Ltd. | Los Angeles | USA | 2004-01-09 | 509 |
| Double Decker Gift Stores, Ltd | London | UK | 2004-01-22 | 496 |
| West Coast Collectables Co. | Burbank | USA | 2004-01-29 | 489 |
| Frau da Collezione | Milan | Italy | 2004-02-09 | 478 |
| Signal Collectibles Ltd. | Brisbane | USA | 2004-02-10 | 477 |
| Daedalus Designs Imports | Lille | France | 2004-02-21 | 466 |
| Collectable Mini Designs Co. | San Diego | USA | 2004-02-26 | 461 |
| Saveley & Henriot, Co. | Lyon | France | 2004-03-02 | 456 |
| CAF Imports | Madrid | Spain | 2004-03-19 | 439 |
| Osaka Souveniers Co. | Kita-ku | Japan | 2004-04-13 | 414 |
| Diecast Collectables | Boston | USA | 2004-04-26 | 401 |
| Super Scale Inc. | New Haven | USA | 2004-05-04 | 393 |
| Cambridge Collectables Co. | Cambridge | USA | 2004-05-08 | 389 |
| Royal Canadian Collectables, Ltd. | Tsawassen | Canada | 2004-08-20 | 285 |

> **Recommendation:** Note that Saveley & Henriot (Lyon, France) is a **Silver** customer dormant for 456 days — a prime re-engagement target. A personalised outreach campaign for these customers could recover significant lost revenue.

---

## Summary of Advanced SQL Techniques Used

| Technique | Used In |
|---|---|
| 5-table `JOIN` | 1a, 1c |
| Self-join (same table joined to itself) | 1b |
| `LEFT JOIN` | 1b |
| String concatenation (`\|\|`) | 1a, 1b |
| `COUNT(DISTINCT ...)` | 1a, 1c, 3a, 3b |
| `strftime()` — date extraction | 2a, 2b |
| `julianday()` — date arithmetic | 2c, 3c |
| `WHERE` with `IS NOT NULL` | 2c |
| `CASE WHEN` — conditional segmentation | 3a, 3b |
| Chained CTEs (`WITH … AS`) | 3a, 3b |
| `HAVING` — filtering aggregated results | 3c |
| `AVG`, `MIN`, `MAX` | 2c, 3b |

---

*Phase 2 of the Scale Model Car Store SQL Analysis — `stores.db`.*
