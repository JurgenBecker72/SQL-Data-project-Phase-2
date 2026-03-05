# Scale Model Car Store SQL Analysis
# Phase 1 & 2 – Inventory, Customer & Commercial Intelligence

![Phase 2 Analytics Dashboard](phase2_dashboard.svg)

> 📄 **[Download full Phase 2 Analytics Infographic (PDF)](SQL_Phase2_Dashboard_v4.pdf)**

---

This repository contains a two-phase SQL analysis of a scale model car retailer using a relational SQLite database (`stores.db`). The project answers key business questions around **inventory management**, **product performance**, **customer value**, **sales rep effectiveness** and **revenue trends** — providing actionable recommendations to inform sales strategy and commercial decision-making.

---

## What this analysis shows

- **Inventory health** — low stock ratios identifying products that need urgent restocking
- **Product performance** — revenue and profit rankings across the full product catalogue
- **Priority restock list** — products that are both high-revenue *and* critically low in stock, identified using chained CTEs
- **VIP customer identification** — top 10 customers ranked by profit contribution, with full contact details for targeted outreach
- **Sales rep & office performance** — profit, order volume and customer count per rep and per office globally
- **Management hierarchy** — full organisational chart built using a self-join on the employees table
- **Monthly revenue trends** — seasonal patterns and year-on-year product line performance using date functions
- **Customer segmentation** — Platinum / Gold / Silver / Bronze tiers using `CASE WHEN`, with churn-risk customers identified for re-engagement

---

## Key concepts

The analysis is structured across two phases, each building on the previous:

1. **Phase 1 — Sales Strategy**: Focuses on inventory and customer value. Identifies what to restock and who the most profitable customers are.

2. **Phase 2 — Advanced Analytics**: Extends the analysis to people and time. Evaluates which sales reps and offices drive the most value, how revenue trends seasonally, and how customers can be segmented for targeted strategy.

3. **Business recommendations** flow from both phases — covering restocking priorities, VIP marketing, rep incentives, Q4 planning and customer re-engagement campaigns.

---

## Data sources

- **stores.db** — a SQLite relational database containing 8 tables and approximately 4,100 rows of transactional, customer, product and employee data
- No external data was used — all analysis is self-contained within the database
- All queries are verified against the live database and outputs are documented in the notebook files

---

## Methods (high level)

**Phase 1:**
- Calculated a **low stock ratio** (units ordered ÷ units in stock) to rank inventory urgency
- Used **correlated subqueries** to compute product revenue performance
- Applied **chained CTEs** to intersect low-stock and high-revenue products for a priority restock list
- Joined four tables to calculate **profit per customer** (`priceEach − buyPrice × quantityOrdered`) and rank top VIP accounts

**Phase 2:**
- Used a **self-join** on the employees table to map the full management hierarchy
- Applied `strftime()` and `julianday()` for **date-based aggregation** and fulfilment time analysis
- Implemented **`CASE WHEN` segmentation** inside a CTE to classify customers into value tiers
- Used `HAVING` and `MAX(orderDate)` to identify **dormant customers** at churn risk

---

## Important notes

- All data is from a structured relational database — no simulated or external data is used
- Profit calculations use `priceEach − buyPrice` as the margin proxy; no operating cost data is available in the database
- The dataset covers orders from January 2003 to May 2005 — 2005 figures are partial
- Customer segmentation thresholds (Platinum / Gold / Silver / Bronze) are set based on the distribution of profits in this dataset and are illustrative

---

## How to view the report

The full analysis notebooks are available in this repository:

- **Phase 1:** `stores_analysis_notebook.md`
- **Phase 2:** `phase2_advanced_analytics_notebook.md`

Each notebook contains the SQL code, plain-English explanation and verified query output side by side.

---

## Interactive Dashboard (Plotly Dash)

Phase 2 findings are available as a fully interactive Plotly Dash dashboard.

**File:** `app.py`

**Run the dashboard:**
```bash
pip install dash plotly pandas
python app.py
```
Then open **http://localhost:8050** in your browser.

**Dashboard tabs:**

| Tab | Content |
|---|---|
| 👤 Sales Reps & Offices | Rep profit rankings, office comparison, fulfilment time, management hierarchy table |
| 📈 Revenue Trends | Monthly revenue line chart, product line annual comparison, monthly detail table |
| 🎯 Customer Segments | Segment donut + profit bar, filterable customer table, churn-risk dormant customers |

The dashboard reads directly from `stores.db` and requires no external data.

---

## SQL techniques used

- **Querying**: `SELECT` · `WHERE` · `GROUP BY` · `ORDER BY` · `LIMIT` · `HAVING`
- **Joins**: Multi-table `JOIN` (up to 5 tables) · `LEFT JOIN` · Self-join
- **Subqueries & CTEs**: Correlated subqueries · `WITH … AS` (chained CTEs)
- **Aggregation**: `SUM` · `COUNT(DISTINCT …)` · `AVG` · `MIN` · `MAX` · `ROUND`
- **Date functions**: `strftime()` · `julianday()`
- **Conditional logic**: `CASE WHEN`
- **Set operations**: `UNION ALL`
- **Database utilities**: `PRAGMA_TABLE_INFO` · `CREATE TEMP TABLE`
- **String operations**: Concatenation with `||`

**Tool:** VS Code + SQLite extension (alexcvzz) · SQLite 3

---

*AX Consult Group.*
