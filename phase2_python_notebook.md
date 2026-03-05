# Phase 2 — Advanced Analytics · Python Notebook

**Database:** `stores.db` (SQLite) &nbsp;|&nbsp; **Author:** Jurgen B.
**Stack:** Python 3 · pandas · sqlite3 · matplotlib · seaborn

> This notebook reproduces the full Phase 2 analysis using native Python.
> Each section shows the equivalent pandas/Python code alongside the output,
> mirroring the SQL notebook for direct comparison.

---

## Setup — Connect to Database

```python
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

# ── Connect
conn = sqlite3.connect("stores.db")

# ── Global plot style
sns.set_theme(style="darkgrid", palette="muted")
plt.rcParams["figure.dpi"] = 130
```

---

## Analysis 1 — Sales Rep & Manager Performance

---

### 1a. Sales Rep Performance

**Python approach:** Load the five tables individually, then chain `pd.merge()` calls to replicate the multi-table JOIN. Aggregate with `.groupby()`.

```python
# ── Load tables
employees    = pd.read_sql("SELECT * FROM employees",    conn)
offices      = pd.read_sql("SELECT * FROM offices",      conn)
customers    = pd.read_sql("SELECT * FROM customers",    conn)
orders       = pd.read_sql("SELECT * FROM orders",       conn)
orderdetails = pd.read_sql("SELECT * FROM orderdetails", conn)
products     = pd.read_sql("SELECT * FROM products",     conn)

# ── Build the joined DataFrame (mirrors 5-table SQL JOIN)
rep_data = (
    employees
    .merge(offices,      on="officeCode",              how="inner")
    .merge(customers,    left_on="employeeNumber",
                         right_on="salesRepEmployeeNumber", how="inner")
    .merge(orders,       on="customerNumber",           how="inner")
    .merge(orderdetails, on="orderNumber",              how="inner")
    .merge(products,     on="productCode",              how="inner")
)

# ── Calculate profit per line item
rep_data["line_profit"] = (
    rep_data["quantityOrdered"] * (rep_data["priceEach"] - rep_data["buyPrice"])
)

# ── Aggregate per sales rep
rep_summary = (
    rep_data
    .groupby(["employeeNumber", "firstName", "lastName", "city"])
    .agg(
        total_customers=("customerNumber",  "nunique"),
        total_orders   =("orderNumber",     "nunique"),
        total_profit   =("line_profit",     "sum"),
    )
    .reset_index()
    .assign(
        sales_rep   = lambda d: d["firstName"] + " " + d["lastName"],
        total_profit= lambda d: d["total_profit"].round(2),
    )
    .rename(columns={"city": "office"})
    [["sales_rep", "office", "total_customers", "total_orders", "total_profit"]]
    .sort_values("total_profit", ascending=False)
    .reset_index(drop=True)
)

print(rep_summary.to_string(index=False))
```

**Output:**

| sales_rep | office | total_customers | total_orders | total_profit |
|---|---|---|---|---|
| Gerard Hernandez | Paris | 7 | 43 | 504644.71 |
| Leslie Jennings | San Francisco | 6 | 34 | 435208.35 |
| Pamela Castillo | Paris | 10 | 31 | 340727.90 |
| Larry Bott | London | 8 | 22 | 290203.59 |
| Barry Jones | London | 9 | 25 | 276659.25 |
| George Vanauf | NYC | 8 | 22 | 269596.09 |
| Loui Bondur | Paris | 6 | 20 | 234891.07 |
| Peter Marsh | Sydney | 5 | 19 | 230811.75 |
| Andy Fixter | Sydney | 5 | 19 | 222207.18 |
| Steve Patterson | Boston | 6 | 18 | 197879.23 |
| Foon Yue Tseng | NYC | 6 | 17 | 194839.92 |
| Mami Nishi | Tokyo | 5 | 16 | 181181.80 |
| Martin Gerard | Paris | 5 | 12 | 156878.63 |
| Julie Firrelli | Boston | 6 | 14 | 152119.31 |
| Leslie Thompson | San Francisco | 6 | 14 | 138031.47 |

```python
# ── Visualise: horizontal bar chart of rep profit
fig, ax = plt.subplots(figsize=(9, 6))
colors = ["#2d6a4f" if v == rep_summary["total_profit"].max() else "#4a90e2"
          for v in rep_summary["total_profit"]]
ax.barh(rep_summary["sales_rep"], rep_summary["total_profit"] / 1000,
        color=colors, edgecolor="none")
ax.set_xlabel("Total Profit ($000s)")
ax.set_title("Sales Rep Performance — Total Profit", fontweight="bold")
ax.invert_yaxis()
plt.tight_layout()
plt.show()
```

> **Finding:** Gerard Hernandez (Paris) leads at $504K despite the smallest customer list — highest profit-per-customer on the team.

---

### 1b. Management Hierarchy (Self-Join)

**Python approach:** A self-join on a single DataFrame — merge `employees` to itself on `reportsTo → employeeNumber`.

```python
# ── Self-merge: employee to their manager
emp_base    = employees[["employeeNumber", "firstName", "lastName",
                          "jobTitle", "reportsTo", "officeCode"]].copy()
emp_manager = employees[["employeeNumber", "firstName", "lastName",
                          "jobTitle"]].copy()
emp_manager.columns = ["mgr_number", "mgr_first", "mgr_last", "mgr_title"]

hierarchy = (
    emp_base
    .merge(emp_manager,
           left_on="reportsTo", right_on="mgr_number",
           how="left")          # LEFT to preserve the President (no manager)
    .merge(offices[["officeCode", "city"]], on="officeCode")
    .assign(
        employee   = lambda d: d["firstName"] + " " + d["lastName"],
        reports_to = lambda d: (d["mgr_first"] + " " + d["mgr_last"])
                               .where(d["mgr_number"].notna(), "—"),
    )
    [["employee", "jobTitle", "reports_to", "mgr_title", "city"]]
    .rename(columns={"jobTitle": "job_title",
                     "mgr_title": "manager_title"})
    .sort_values(["reports_to", "employee"])
    .reset_index(drop=True)
)

print(hierarchy.to_string(index=False))
```

**Output:**

| employee | job_title | reports_to | manager_title | city |
|---|---|---|---|---|
| Diane Murphy | President | — | — | San Francisco |
| Mary Patterson | VP Sales | Diane Murphy | President | San Francisco |
| Jeff Firrelli | VP Marketing | Diane Murphy | President | San Francisco |
| Anthony Bow | Sales Manager (NA) | Mary Patterson | VP Sales | San Francisco |
| Gerard Bondur | Sales Manager (EMEA) | Mary Patterson | VP Sales | Paris |
| William Patterson | Sales Manager (APAC) | Mary Patterson | VP Sales | Sydney |
| Leslie Jennings | Sales Rep | Anthony Bow | Sales Manager (NA) | San Francisco |
| ... | ... | ... | ... | ... |

> **Note:** `how="left"` in the merge replicates `LEFT JOIN` — Diane Murphy (no manager) is preserved with NaN, then replaced with `"—"`.

---

### 1c. Office Performance

```python
# ── Roll up to office level (mirrors 5-table SQL GROUP BY officeCode)
office_summary = (
    rep_data
    .groupby(["officeCode", "city", "country", "territory"])
    .agg(
        num_reps      =("employeeNumber", "nunique"),
        num_customers =("customerNumber", "nunique"),
        total_profit  =("line_profit",    "sum"),
    )
    .reset_index()
    .assign(total_profit=lambda d: d["total_profit"].round(2))
    .sort_values("total_profit", ascending=False)
    .reset_index(drop=True)
)

print(office_summary[["city", "country", "territory",
                        "num_reps", "num_customers", "total_profit"]]
      .to_string(index=False))
```

**Output:**

| city | country | territory | num_reps | num_customers | total_profit |
|---|---|---|---|---|---|
| Paris | France | EMEA | 4 | 28 | 1237142.31 |
| San Francisco | USA | NA | 2 | 12 | 573239.82 |
| London | UK | EMEA | 2 | 17 | 566862.84 |
| NYC | USA | NA | 2 | 14 | 464436.01 |
| Sydney | Australia | APAC | 2 | 10 | 453018.93 |
| Boston | USA | NA | 2 | 12 | 349998.54 |
| Tokyo | Japan | Japan | 1 | 5 | 181181.80 |

```python
# ── Visualise: stacked bar — profit vs customers per office
fig, ax1 = plt.subplots(figsize=(8, 4))
ax2 = ax1.twinx()
x = range(len(office_summary))
ax1.bar(x, office_summary["total_profit"] / 1000,
        color="#1d3557", alpha=0.8, label="Profit ($000s)")
ax2.plot(x, office_summary["num_customers"],
         color="#50c8a0", marker="o", linewidth=2, label="Customers")
ax1.set_xticks(x)
ax1.set_xticklabels(office_summary["city"], rotation=30)
ax1.set_ylabel("Total Profit ($000s)")
ax2.set_ylabel("Customers")
ax1.set_title("Office Performance", fontweight="bold")
plt.tight_layout()
plt.show()
```

---

## Analysis 2 — Monthly Revenue Trends

---

### 2a. Monthly Revenue & Profit

**Python approach:** Parse `orderDate` as datetime, extract year/month, filter cancelled orders, then `.groupby()`.

```python
# ── Load and prepare orders
orders_full = (
    pd.read_sql("SELECT * FROM orders", conn)
    .assign(orderDate=lambda d: pd.to_datetime(d["orderDate"]))
)

# ── Build detailed line-item table (exclude cancelled)
revenue_data = (
    orders_full[orders_full["status"] != "Cancelled"]
    .merge(orderdetails, on="orderNumber")
    .merge(products[["productCode", "buyPrice"]], on="productCode")
    .assign(
        year        = lambda d: d["orderDate"].dt.strftime("%Y"),
        month       = lambda d: d["orderDate"].dt.strftime("%m"),
        line_revenue= lambda d: d["quantityOrdered"] * d["priceEach"],
        line_profit = lambda d: d["quantityOrdered"] * (d["priceEach"] - d["buyPrice"]),
    )
)

# ── Monthly aggregation
monthly = (
    revenue_data
    .groupby(["year", "month"])
    .agg(
        num_orders=("orderNumber", "nunique"),
        revenue   =("line_revenue", "sum"),
        profit    =("line_profit",  "sum"),
    )
    .reset_index()
    .assign(
        revenue=lambda d: d["revenue"].round(2),
        profit =lambda d: d["profit"].round(2),
        period =lambda d: pd.to_datetime(d["year"] + "-" + d["month"]),
    )
    .sort_values("period")
    .reset_index(drop=True)
)

print(monthly[["year", "month", "num_orders", "revenue", "profit"]]
      .to_string(index=False))
```

**Output (selected rows):**

| year | month | num_orders | revenue | profit |
|---|---|---|---|---|
| 2003 | 01 | 5 | 116692.77 | 45820.95 |
| 2003 | 09 | 8 | 236697.85 | 93855.53 |
| 2003 | 10 | 17 | 470169.12 | 184166.36 |
| 2003 | 11 | 29 | 965061.55 | 385538.43 |
| 2004 | 11 | 33 | 979291.98 | 392370.92 |
| 2005 | 05 | 15 | 441474.94 | 172133.80 |

```python
# ── Visualise: revenue trend line coloured by year
fig, ax = plt.subplots(figsize=(11, 4))
for yr, grp in monthly.groupby("year"):
    ax.plot(grp["period"], grp["revenue"] / 1000,
            marker="o", markersize=4, label=yr)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}k"))
ax.set_title("Monthly Revenue — 2003 to 2005", fontweight="bold")
ax.set_xlabel("")
ax.legend(title="Year")
plt.tight_layout()
plt.show()
```

> **Finding:** November is the peak month in both 2003 and 2004 — nearly **8× higher** than January. Classic Q4 seasonality.

---

### 2b. Revenue by Product Line per Year

```python
# ── Merge product line into revenue_data
products_pl = pd.read_sql("SELECT productCode, productLine FROM products", conn)
pl_data = revenue_data.merge(products_pl, on="productCode", suffixes=("", "_pl"))

# ── Annual aggregation by product line
product_line_trend = (
    pl_data
    .groupby(["year", "productLine"])
    .agg(revenue=("line_revenue", "sum"), profit=("line_profit", "sum"))
    .reset_index()
    .assign(
        revenue=lambda d: d["revenue"].round(2),
        profit =lambda d: d["profit"].round(2),
    )
    .sort_values(["year", "profit"], ascending=[True, False])
)

print(product_line_trend.to_string(index=False))
```

**Output:**

| year | productLine | revenue | profit |
|---|---|---|---|
| 2003 | Classic Cars | 1369386.78 | 540176.33 |
| 2003 | Vintage Cars | 607535.32 | 251750.18 |
| 2003 | Trucks and Buses | 376657.12 | 147415.65 |
| 2003 | Motorcycles | 344998.74 | 144307.16 |
| 2004 | Classic Cars | 1689307.07 | 673958.15 |
| 2004 | Vintage Cars | 823927.95 | 337219.36 |
| 2005 | Classic Cars | 715953.54 | 280523.06 |
| ... | ... | ... | ... |

```python
# ── Visualise: grouped bar by year per product line
pivot = product_line_trend.pivot(index="productLine",
                                  columns="year", values="profit") / 1000
pivot.sort_values("2004", ascending=False).plot(
    kind="bar", figsize=(9, 4), colormap="Blues",
    edgecolor="white", width=0.7
)
plt.ylabel("Profit ($000s)")
plt.title("Profit by Product Line — Annual Comparison", fontweight="bold")
plt.xticks(rotation=25)
plt.tight_layout()
plt.show()
```

---

### 2c. Order Fulfilment Time

**Python approach:** Convert date columns with `pd.to_datetime()`, compute difference in days using `.dt.days`.

```python
# ── Reload orders with both date columns
orders_dates = pd.read_sql(
    "SELECT status, orderDate, shippedDate FROM orders WHERE shippedDate IS NOT NULL",
    conn,
    parse_dates=["orderDate", "shippedDate"]
)

# ── Days to ship
orders_dates["days_to_ship"] = (
    (orders_dates["shippedDate"] - orders_dates["orderDate"]).dt.days
)

# ── Aggregate by status
fulfilment = (
    orders_dates
    .groupby("status")["days_to_ship"]
    .agg(num_orders="count",
         avg_days=lambda x: round(x.mean(), 1),
         min_days="min",
         max_days="max")
    .reset_index()
    .sort_values("num_orders", ascending=False)
)

print(fulfilment.to_string(index=False))
```

**Output:**

| status | num_orders | avg_days | min_days | max_days |
|---|---|---|---|---|
| Shipped | 303 | 3.8 | 1 | 65 |
| Resolved | 4 | 3.5 | 2 | 5 |
| Disputed | 3 | 5.0 | 3 | 6 |
| Cancelled | 2 | 1.5 | 1 | 2 |

> **Finding:** Average fulfilment is 3.8 days — healthy. The 65-day max is a significant outlier worth investigating.

---

## Analysis 3 — Customer Segmentation

---

### 3a. Customer Segmentation

**Python approach:** Replicate the CTE as an intermediate DataFrame, apply `np.select()` to assign tiers (the pandas equivalent of `CASE WHEN`).

```python
# ── Customer profit DataFrame
customers_df = pd.read_sql("SELECT * FROM customers", conn)

cust_profit = (
    customers_df
    .merge(orders_full[orders_full["status"] != "Cancelled"][["orderNumber", "customerNumber"]],
           on="customerNumber")
    .merge(orderdetails, on="orderNumber")
    .merge(products[["productCode", "buyPrice"]], on="productCode")
    .assign(line_profit=lambda d: d["quantityOrdered"] * (d["priceEach"] - d["buyPrice"]))
    .groupby(["customerNumber", "customerName", "city", "country"])
    .agg(total_profit=("line_profit", "sum"),
         total_orders=("orderNumber", "nunique"))
    .reset_index()
    .assign(total_profit=lambda d: d["total_profit"].round(2))
)

# ── CASE WHEN via np.select
conditions = [
    cust_profit["total_profit"] >= 100_000,
    cust_profit["total_profit"] >= 50_000,
    cust_profit["total_profit"] >= 20_000,
]
choices = ["Platinum", "Gold", "Silver"]
cust_profit["segment"] = np.select(conditions, choices, default="Bronze")

result = cust_profit.sort_values("total_profit", ascending=False).reset_index(drop=True)
print(result[["customerName", "city", "country",
               "total_profit", "total_orders", "segment"]]
      .head(20).to_string(index=False))
```

**Output (Top 20):**

| customerName | city | country | total_profit | total_orders | segment |
|---|---|---|---|---|---|
| Euro+ Shopping Channel | Madrid | Spain | 326519.66 | 26 | Platinum |
| Mini Gifts Distributors Ltd. | San Rafael | USA | 236769.39 | 17 | Platinum |
| Muscle Machine Inc | NYC | USA | 72370.09 | 4 | Gold |
| Australian Collectors, Co. | Melbourne | Australia | 70311.07 | 5 | Gold |
| La Rochelle Gifts | Nantes | France | 60875.30 | 4 | Gold |
| Dragon Souveniers, Ltd. | Singapore | Singapore | 60477.38 | 5 | Gold |
| AV Stores, Co. | Manchester | UK | 60095.86 | 3 | Gold |
| Down Under Souveniers, Inc | Auckland | New Zealand | 60013.99 | 5 | Gold |
| ... | ... | ... | ... | ... | ... |

---

### 3b. Segment Summary

```python
# ── Group by segment — mirrors the chained CTE
segment_summary = (
    cust_profit
    .groupby("segment")
    .agg(
        num_customers=("customerNumber", "count"),
        avg_profit   =("total_profit",   "mean"),
        total_profit =("total_profit",   "sum"),
        avg_orders   =("total_orders",   "mean"),
    )
    .reset_index()
    .assign(
        avg_profit  =lambda d: d["avg_profit"].round(2),
        total_profit=lambda d: d["total_profit"].round(2),
        avg_orders  =lambda d: d["avg_orders"].round(1),
    )
    .sort_values("total_profit", ascending=False)
)

print(segment_summary.to_string(index=False))
```

**Output:**

| segment | num_customers | avg_profit | total_profit | avg_orders |
|---|---|---|---|---|
| Silver | 62 | 33102.25 | 2052339.38 | 2.8 |
| Gold | 17 | 57470.05 | 976990.89 | 4.1 |
| Platinum | 2 | 281644.53 | 563289.05 | 21.5 |
| Bronze | 17 | 13721.23 | 233260.93 | 2.3 |

```python
# ── Visualise: donut chart of customer segment distribution
segment_order = ["Platinum", "Gold", "Silver", "Bronze"]
palette = {"Platinum": "#ffd700", "Gold": "#50c8a0",
           "Silver": "#4a90e2",   "Bronze": "#ff7c7c"}

seg_plot = segment_summary.set_index("segment").loc[segment_order]
fig, ax = plt.subplots(figsize=(5, 5))
wedges, texts, autotexts = ax.pie(
    seg_plot["num_customers"],
    labels=seg_plot.index,
    colors=[palette[s] for s in seg_plot.index],
    autopct="%1.0f%%",
    startangle=90,
    wedgeprops={"width": 0.55, "edgecolor": "white", "linewidth": 2},
)
ax.set_title("Customer Segments — Count", fontweight="bold")
plt.tight_layout()
plt.show()
```

> **Finding:** Just 2 Platinum customers generate $563K — more than the entire Bronze tier of 17 customers.

---

### 3c. Dormant Customers (Churn Risk)

**Python approach:** Use `pd.to_datetime()` and date arithmetic; filter using `.query()` — mirrors `HAVING days_since_order > 180`.

```python
# ── Dataset end date (mirrors julianday('2005-06-01'))
DATASET_END = pd.Timestamp("2005-06-01")

dormant = (
    orders_full
    .groupby("customerNumber")["orderDate"]
    .max()
    .reset_index()
    .rename(columns={"orderDate": "last_order_date"})
    .assign(
        days_since_order=lambda d: (DATASET_END - d["last_order_date"]).dt.days
    )
    .query("days_since_order > 180")
    .merge(customers_df[["customerNumber", "customerName", "city", "country"]],
           on="customerNumber")
    .sort_values("days_since_order", ascending=False)
    .reset_index(drop=True)
    [["customerName", "city", "country", "last_order_date", "days_since_order"]]
)

# ── Flag Silver-tier dormant customers (highest re-engagement value)
dormant = dormant.merge(
    cust_profit[["customerName", "segment"]], on="customerName", how="left"
)

print(dormant.head(15).to_string(index=False))
```

**Output (Top 15):**

| customerName | city | country | last_order_date | days_since_order | segment |
|---|---|---|---|---|---|
| King Kong Collectables, Co. | Hong Kong | Hong Kong | 2003-12-01 | 548 | Bronze |
| Men 'R' US Retailers, Ltd. | Los Angeles | USA | 2004-01-09 | 509 | Silver |
| Double Decker Gift Stores, Ltd | London | UK | 2004-01-22 | 496 | Bronze |
| West Coast Collectables Co. | Burbank | USA | 2004-01-29 | 489 | Bronze |
| Frau da Collezione | Milan | Italy | 2004-02-09 | 478 | Silver |
| Signal Collectibles Ltd. | Brisbane | USA | 2004-02-10 | 477 | Bronze |
| Daedalus Designs Imports | Lille | France | 2004-02-21 | 466 | Bronze |
| Collectable Mini Designs Co. | San Diego | USA | 2004-02-26 | 461 | Silver |
| Saveley & Henriot, Co. | Lyon | France | 2004-03-02 | 456 | Gold |
| CAF Imports | Madrid | Spain | 2004-03-19 | 439 | Bronze |
| Osaka Souveniers Co. | Kita-ku | Japan | 2004-04-13 | 414 | Bronze |
| Diecast Collectables | Boston | USA | 2004-04-26 | 401 | Bronze |
| Super Scale Inc. | New Haven | USA | 2004-05-04 | 393 | Bronze |
| Cambridge Collectables Co. | Cambridge | USA | 2004-05-08 | 389 | Bronze |
| Royal Canadian Collectables, Ltd. | Tsawassen | Canada | 2004-08-20 | 285 | Silver |

> **Recommendation:** Saveley & Henriot (Gold tier, 456 days dormant) is the highest-value re-engagement target.

---

## Cleanup

```python
conn.close()
print("Connection closed.")
```

---

## SQL → Python Translation Reference

| SQL Construct | Python Equivalent |
|---|---|
| `JOIN … ON` | `pd.merge(left, right, left_on=..., right_on=...)` |
| `LEFT JOIN` | `pd.merge(..., how="left")` |
| `GROUP BY … COUNT(DISTINCT)` | `.groupby().agg(col=("col", "nunique"))` |
| `SUM`, `AVG`, `MIN`, `MAX` | `.agg(sum / mean / min / max)` |
| `ORDER BY col DESC` | `.sort_values("col", ascending=False)` |
| `WHERE col != 'X'` | `.query("col != 'X'")` or boolean indexing |
| `HAVING agg > val` | filter after `.groupby().agg()` |
| `CASE WHEN` | `np.select(conditions, choices, default=...)` |
| `WITH … AS (CTE)` | intermediate DataFrame variable |
| `strftime('%Y', date)` | `pd.to_datetime(col).dt.strftime("%Y")` |
| `julianday(d2) - julianday(d1)` | `(d2 - d1).dt.days` |
| `\|\|` (concatenation) | `df["a"] + " " + df["b"]` |

---

*Phase 2 · Python Notebook — `stores.db` · AX Consult Group*
