"""
Phase 2 Advanced Analytics Dashboard
Scale Model Car Store — stores.db
Author: Jurgen B. · AX Consult Group

Run:
    pip install dash plotly pandas
    python app.py
Then open http://localhost:8050
"""

import sqlite3
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from dash import Dash, dcc, html, dash_table, Input, Output
import warnings
warnings.filterwarnings("ignore")

# ═══════════════════════════════════════════════════════════════
# 0.  DATABASE HELPERS
# ═══════════════════════════════════════════════════════════════

DB_PATH = "stores.db"

def get_conn():
    return sqlite3.connect(DB_PATH)


def load_tables():
    conn = get_conn()
    tbls = {}
    for t in ["employees", "offices", "customers", "orders",
              "orderdetails", "products"]:
        tbls[t] = pd.read_sql(f"SELECT * FROM {t}", conn)
    conn.close()
    tbls["orders"]["orderDate"]    = pd.to_datetime(tbls["orders"]["orderDate"])
    tbls["orders"]["shippedDate"]  = pd.to_datetime(tbls["orders"]["shippedDate"],
                                                     errors="coerce")
    return tbls


# ═══════════════════════════════════════════════════════════════
# 1.  DATA PROCESSING
# ═══════════════════════════════════════════════════════════════

tbls = load_tables()
employees, offices, customers, orders, orderdetails, products = (
    tbls["employees"], tbls["offices"], tbls["customers"],
    tbls["orders"],    tbls["orderdetails"], tbls["products"],
)

# ── Base joined frame (used across several analyses)
base = (
    employees
    .merge(offices,      on="officeCode")
    .merge(customers,    left_on="employeeNumber",
                         right_on="salesRepEmployeeNumber")
    .merge(orders,       on="customerNumber")
    .merge(orderdetails, on="orderNumber")
    .merge(products,     on="productCode")
)
base["line_profit"]  = base["quantityOrdered"] * (base["priceEach"] - base["buyPrice"])
base["line_revenue"] = base["quantityOrdered"] * base["priceEach"]

# ── 1a  Sales Rep Performance
rep_summary = (
    base.groupby(["employeeNumber",
                  "firstName", "lastName",
                  "city_x"],  # city_x = office city after merge
         as_index=False)
    .agg(customers=("customerNumber",  "nunique"),
         orders   =("orderNumber",     "nunique"),
         profit   =("line_profit",     "sum"))
    .assign(
        sales_rep=lambda d: d["firstName"] + " " + d["lastName"],
        profit   =lambda d: d["profit"].round(2),
        office   =lambda d: d["city_x"],
    )
    .sort_values("profit", ascending=False)
    .reset_index(drop=True)
)

# ── 1b  Management Hierarchy (self-join)
mgr = employees[["employeeNumber","firstName","lastName","jobTitle"]].copy()
mgr.columns = ["mgr_num","mgr_first","mgr_last","mgr_title"]
hierarchy = (
    employees[["employeeNumber","firstName","lastName","jobTitle",
               "reportsTo","officeCode"]]
    .merge(mgr,    left_on="reportsTo", right_on="mgr_num", how="left")
    .merge(offices[["officeCode","city"]], on="officeCode")
    .assign(
        employee   =lambda d: d["firstName"]+" "+d["lastName"],
        reports_to =lambda d: (d["mgr_first"]+" "+d["mgr_last"])
                               .where(d["mgr_num"].notna(), "—"),
        mgr_title  =lambda d: d["mgr_title"].fillna("—"),
    )
    [["employee","jobTitle","reports_to","mgr_title","city"]]
    .rename(columns={"jobTitle":"job_title","mgr_title":"manager_title"})
    .sort_values(["reports_to","employee"])
    .reset_index(drop=True)
)

# ── 1c  Office Performance
office_summary = (
    base.groupby(["officeCode","city_x","country_x","territory"],
                  as_index=False)
    .agg(num_reps     =("employeeNumber","nunique"),
         num_customers=("customerNumber","nunique"),
         profit       =("line_profit",   "sum"))
    .assign(profit=lambda d: d["profit"].round(2))
    .rename(columns={"city_x":"city","country_x":"country"})
    .sort_values("profit", ascending=False)
    .reset_index(drop=True)
)

# ── 2a  Monthly Revenue
rev_data = (
    orders[orders["status"] != "Cancelled"]
    .merge(orderdetails, on="orderNumber")
    .merge(products[["productCode","buyPrice","productLine"]], on="productCode")
    .assign(
        year        =lambda d: d["orderDate"].dt.year.astype(str),
        month       =lambda d: d["orderDate"].dt.month,
        line_revenue=lambda d: d["quantityOrdered"] * d["priceEach"],
        line_profit =lambda d: d["quantityOrdered"] * (d["priceEach"] - d["buyPrice"]),
        period      =lambda d: pd.to_datetime(
                        d["orderDate"].dt.strftime("%Y-%m")),
    )
)

monthly = (
    rev_data.groupby(["year","month","period"], as_index=False)
    .agg(num_orders=("orderNumber","nunique"),
         revenue   =("line_revenue","sum"),
         profit    =("line_profit", "sum"))
    .assign(revenue=lambda d: d["revenue"].round(2),
            profit =lambda d: d["profit"].round(2))
    .sort_values("period")
)

# ── 2b  Product Line Trends
pl_annual = (
    rev_data.groupby(["year","productLine"], as_index=False)
    .agg(revenue=("line_revenue","sum"), profit=("line_profit","sum"))
    .assign(revenue=lambda d: d["revenue"].round(2),
            profit =lambda d: d["profit"].round(2))
    .sort_values(["year","profit"], ascending=[True, False])
)

# ── 2c  Fulfilment Time
ship_data = (
    orders[orders["shippedDate"].notna()]
    .assign(days=lambda d: (d["shippedDate"]-d["orderDate"]).dt.days)
)
fulfilment = (
    ship_data.groupby("status")["days"]
    .agg(num_orders="count",
         avg_days  =lambda x: round(x.mean(),1),
         min_days  ="min",
         max_days  ="max")
    .reset_index()
    .sort_values("num_orders", ascending=False)
)

# ── 3a  Customer Segmentation
DATASET_END = pd.Timestamp("2005-06-01")
cust_profit = (
    orders[orders["status"] != "Cancelled"][["orderNumber","customerNumber"]]
    .merge(orderdetails, on="orderNumber")
    .merge(products[["productCode","buyPrice"]], on="productCode")
    .assign(line_profit=lambda d: d["quantityOrdered"]*(d["priceEach"]-d["buyPrice"]))
    .groupby("customerNumber", as_index=False)
    .agg(total_profit=("line_profit","sum"),
         total_orders=("orderNumber","nunique"))
    .merge(customers[["customerNumber","customerName","city","country"]], on="customerNumber")
    .assign(
        total_profit=lambda d: d["total_profit"].round(2),
        segment=lambda d: np.select(
            [d["total_profit"]>=100_000,
             d["total_profit"]>= 50_000,
             d["total_profit"]>= 20_000],
            ["Platinum","Gold","Silver"],
            default="Bronze"
        ),
    )
    .sort_values("total_profit", ascending=False)
    .reset_index(drop=True)
)

seg_summary = (
    cust_profit.groupby("segment", as_index=False)
    .agg(num_customers=("customerNumber","count"),
         avg_profit   =("total_profit","mean"),
         total_profit =("total_profit","sum"),
         avg_orders   =("total_orders","mean"))
    .assign(avg_profit  =lambda d: d["avg_profit"].round(2),
            total_profit=lambda d: d["total_profit"].round(2),
            avg_orders  =lambda d: d["avg_orders"].round(1))
)

# ── 3b  Dormant / Churn Risk
last_order = (
    orders.groupby("customerNumber")["orderDate"].max()
    .reset_index()
    .rename(columns={"orderDate":"last_order_date"})
    .assign(days_since=lambda d: (DATASET_END-d["last_order_date"]).dt.days)
    .query("days_since > 180")
    .merge(customers[["customerNumber","customerName","city","country"]], on="customerNumber")
    .merge(cust_profit[["customerNumber","segment","total_profit"]], on="customerNumber", how="left")
    .sort_values("days_since", ascending=False)
    .reset_index(drop=True)
)

# ═══════════════════════════════════════════════════════════════
# 2.  COLOUR PALETTE & THEME
# ═══════════════════════════════════════════════════════════════

COLOURS = {
    "bg"       : "#1a1a2e",
    "panel"    : "#20203a",
    "border"   : "#383860",
    "text"     : "#e0e4ff",
    "subtext"  : "#9aa0cc",
    "green"    : "#50c8a0",
    "blue"     : "#4a90e2",
    "purple"   : "#7c6fff",
    "gold"     : "#ffd700",
    "red"      : "#ff7c7c",
    "orange"   : "#ff8c42",
}

SEG_COLOURS = {
    "Platinum": COLOURS["gold"],
    "Gold"    : COLOURS["green"],
    "Silver"  : COLOURS["blue"],
    "Bronze"  : COLOURS["red"],
}

PLOT_LAYOUT = dict(
    paper_bgcolor=COLOURS["panel"],
    plot_bgcolor =COLOURS["panel"],
    font         =dict(color=COLOURS["text"], family="Segoe UI, Arial, sans-serif"),
    margin       =dict(l=40, r=20, t=40, b=40),
    xaxis        =dict(gridcolor=COLOURS["border"], zerolinecolor=COLOURS["border"]),
    yaxis        =dict(gridcolor=COLOURS["border"], zerolinecolor=COLOURS["border"]),
)


def styled_table(df, id_str, col_fmt=None):
    """Return a dash_table.DataTable styled to match the dark theme."""
    return dash_table.DataTable(
        id=id_str,
        columns=[{"name": c.replace("_"," ").title(), "id": c} for c in df.columns],
        data=df.to_dict("records"),
        style_table={"overflowX": "auto", "borderRadius": "8px"},
        style_cell={
            "backgroundColor": COLOURS["panel"],
            "color"          : COLOURS["text"],
            "border"         : f"1px solid {COLOURS['border']}",
            "padding"        : "8px 14px",
            "fontFamily"     : "Segoe UI, Arial, sans-serif",
            "fontSize"       : "12px",
            "textAlign"      : "left",
        },
        style_header={
            "backgroundColor": COLOURS["bg"],
            "color"          : COLOURS["purple"],
            "fontWeight"     : "bold",
            "border"         : f"1px solid {COLOURS['border']}",
        },
        style_data_conditional=col_fmt or [],
        page_size=20,
        sort_action="native",
        filter_action="native",
    )


# ═══════════════════════════════════════════════════════════════
# 3.  FIGURES
# ═══════════════════════════════════════════════════════════════

# ── Fig 1: Rep profit bar
def fig_rep_profit():
    df = rep_summary.sort_values("profit")
    bar_cols = [COLOURS["green"] if v == df["profit"].max()
                else COLOURS["blue"] for v in df["profit"]]
    fig = go.Figure(go.Bar(
        x=df["profit"], y=df["sales_rep"],
        orientation="h",
        marker_color=bar_cols,
        text=[f"${v/1000:.0f}k" for v in df["profit"]],
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>Profit: $%{x:,.0f}<extra></extra>",
    ))
    fig.update_layout(**PLOT_LAYOUT,
                      title="Sales Rep — Total Profit",
                      height=480,
                      xaxis_title="Total Profit ($)",
                      yaxis_title="")
    return fig


# ── Fig 2: Office profit bar
def fig_office_profit():
    df = office_summary.sort_values("profit")
    fig = go.Figure(go.Bar(
        x=df["profit"], y=df["city"],
        orientation="h",
        marker_color=COLOURS["purple"],
        text=[f"${v/1000:.0f}k" for v in df["profit"]],
        textposition="outside",
        customdata=df[["country","territory","num_reps","num_customers"]].values,
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Country: %{customdata[0]}<br>"
            "Territory: %{customdata[1]}<br>"
            "Reps: %{customdata[2]}<br>"
            "Customers: %{customdata[3]}<br>"
            "Profit: $%{x:,.0f}<extra></extra>"
        ),
    ))
    fig.update_layout(**PLOT_LAYOUT,
                      title="Office Performance — Total Profit",
                      height=340,
                      xaxis_title="Total Profit ($)",
                      yaxis_title="")
    return fig


# ── Fig 3: Monthly revenue line
def fig_monthly_revenue():
    fig = go.Figure()
    palette = {"2003": COLOURS["blue"],
               "2004": COLOURS["green"],
               "2005": COLOURS["purple"]}
    for yr, grp in monthly.groupby("year"):
        fig.add_trace(go.Scatter(
            x=grp["period"], y=grp["revenue"],
            mode="lines+markers",
            name=yr,
            line=dict(color=palette.get(yr, "#fff"), width=2),
            marker=dict(size=5),
            hovertemplate=f"<b>{yr}</b><br>Revenue: $%{{y:,.0f}}<extra></extra>",
        ))
    fig.update_layout(**PLOT_LAYOUT,
                      title="Monthly Revenue — 2003 to 2005",
                      height=320,
                      xaxis_title="",
                      yaxis_title="Revenue ($)",
                      legend=dict(bgcolor=COLOURS["panel"]))
    return fig


# ── Fig 4: Product line grouped bar
def fig_product_line():
    fig = px.bar(
        pl_annual, x="productLine", y="profit", color="year",
        barmode="group",
        color_discrete_map={"2003": COLOURS["blue"],
                             "2004": COLOURS["green"],
                             "2005": COLOURS["purple"]},
        hover_data={"revenue": True},
    )
    fig.update_layout(**PLOT_LAYOUT,
                      title="Profit by Product Line — Annual",
                      height=320,
                      xaxis_title="",
                      yaxis_title="Profit ($)",
                      legend=dict(bgcolor=COLOURS["panel"]))
    return fig


# ── Fig 5: Fulfilment box / bar
def fig_fulfilment():
    fig = go.Figure(go.Bar(
        x=fulfilment["status"],
        y=fulfilment["avg_days"],
        marker_color=[COLOURS["green"] if s == "Shipped" else COLOURS["orange"]
                      for s in fulfilment["status"]],
        text=fulfilment["avg_days"].astype(str) + " days",
        textposition="outside",
        customdata=fulfilment[["num_orders","min_days","max_days"]].values,
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Avg: %{y} days<br>"
            "Orders: %{customdata[0]}<br>"
            "Min: %{customdata[1]} · Max: %{customdata[2]} days"
            "<extra></extra>"
        ),
    ))
    fig.update_layout(**PLOT_LAYOUT,
                      title="Order Fulfilment Time (Avg Days to Ship)",
                      height=280,
                      xaxis_title="",
                      yaxis_title="Avg Days")
    return fig


# ── Fig 6: Segment donut
def fig_seg_donut():
    order_map = ["Platinum", "Gold", "Silver", "Bronze"]
    df = seg_summary.set_index("segment").reindex(order_map).reset_index()
    fig = go.Figure(go.Pie(
        labels=df["segment"],
        values=df["num_customers"],
        hole=0.55,
        marker=dict(colors=[SEG_COLOURS[s] for s in df["segment"]],
                    line=dict(color=COLOURS["bg"], width=2)),
        textinfo="label+percent",
        hovertemplate="<b>%{label}</b><br>Customers: %{value}<br>%{percent}<extra></extra>",
    ))
    fig.update_layout(**PLOT_LAYOUT,
                      title="Customer Segments — Count",
                      height=320,
                      showlegend=False)
    return fig


# ── Fig 7: Segment profit bar
def fig_seg_profit():
    order_map = ["Platinum", "Gold", "Silver", "Bronze"]
    df = seg_summary.set_index("segment").reindex(order_map).reset_index()
    fig = go.Figure(go.Bar(
        x=df["segment"],
        y=df["total_profit"],
        marker_color=[SEG_COLOURS[s] for s in df["segment"]],
        text=[f"${v/1000:.0f}k" for v in df["total_profit"]],
        textposition="outside",
        customdata=df[["num_customers","avg_profit","avg_orders"]].values,
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Total Profit: $%{y:,.0f}<br>"
            "Customers: %{customdata[0]}<br>"
            "Avg Profit: $%{customdata[1]:,.0f}<br>"
            "Avg Orders: %{customdata[2]}"
            "<extra></extra>"
        ),
    ))
    fig.update_layout(**PLOT_LAYOUT,
                      title="Segment Total Profit",
                      height=320,
                      xaxis_title="",
                      yaxis_title="Total Profit ($)")
    return fig


# ─── Dormant scatter
def fig_churn():
    df = last_order.copy()
    df["label"] = df["customerName"].str[:22]
    fig = go.Figure(go.Bar(
        x=df["days_since"].head(15),
        y=df["label"].head(15),
        orientation="h",
        marker_color=[SEG_COLOURS.get(s, COLOURS["subtext"]) for s in df["segment"].head(15)],
        text=df["segment"].head(15),
        textposition="inside",
        customdata=df[["country","total_profit","segment"]].head(15).values,
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Days inactive: %{x}<br>"
            "Country: %{customdata[0]}<br>"
            "Lifetime Profit: $%{customdata[1]:,.0f}<br>"
            "Segment: %{customdata[2]}"
            "<extra></extra>"
        ),
    ))
    fig.update_layout(**PLOT_LAYOUT,
                      title="Top 15 Dormant Customers (>180 days)",
                      height=420,
                      xaxis_title="Days Since Last Order",
                      yaxis_title="")
    return fig


# ═══════════════════════════════════════════════════════════════
# 4.  LAYOUT HELPERS
# ═══════════════════════════════════════════════════════════════

def card(children, style=None):
    base_style = {
        "backgroundColor": COLOURS["panel"],
        "border"         : f"1px solid {COLOURS['border']}",
        "borderRadius"   : "10px",
        "padding"        : "20px",
        "marginBottom"   : "20px",
    }
    if style:
        base_style.update(style)
    return html.Div(children, style=base_style)


def section_title(text):
    return html.H3(text, style={
        "color"       : COLOURS["purple"],
        "fontWeight"  : "600",
        "marginTop"   : "32px",
        "marginBottom": "4px",
        "fontSize"    : "15px",
        "letterSpacing": "0.04em",
    })


def kpi_box(label, value, colour=None):
    return html.Div([
        html.Div(value, style={
            "fontSize"  : "26px",
            "fontWeight": "700",
            "color"     : colour or COLOURS["green"],
        }),
        html.Div(label, style={
            "fontSize": "11px",
            "color"   : COLOURS["subtext"],
            "marginTop": "2px",
        }),
    ], style={
        "backgroundColor": COLOURS["panel"],
        "border"         : f"1px solid {COLOURS['border']}",
        "borderRadius"   : "8px",
        "padding"        : "16px 20px",
        "textAlign"      : "center",
        "flex"           : "1",
    })


# ═══════════════════════════════════════════════════════════════
# 5.  APP LAYOUT
# ═══════════════════════════════════════════════════════════════

app = Dash(__name__, title="Phase 2 Analytics — Scale Model Car Store")

# ── KPI values
total_profit_all = round(base["line_profit"].sum() / 1_000_000, 2)
top_rep          = rep_summary.iloc[0]["sales_rep"]
top_office       = office_summary.iloc[0]["city"]
num_churn        = len(last_order)
pct_platinum     = (seg_summary[seg_summary["segment"]=="Platinum"]["total_profit"].values[0]
                    / seg_summary["total_profit"].sum() * 100)

app.layout = html.Div(style={"backgroundColor": COLOURS["bg"],
                               "minHeight": "100vh",
                               "fontFamily": "Segoe UI, Arial, sans-serif",
                               "color": COLOURS["text"],
                               "padding": "0"}, children=[

    # ── Top header bar
    html.Div(style={
        "backgroundColor": "#252542",
        "padding": "18px 40px",
        "borderBottom": f"2px solid {COLOURS['purple']}",
        "display": "flex",
        "alignItems": "center",
        "justifyContent": "space-between",
    }, children=[
        html.Div([
            html.Span("📊 ", style={"fontSize": "20px"}),
            html.Span("Scale Model Car Store", style={
                "fontSize": "18px", "fontWeight": "700",
                "color": COLOURS["text"],
            }),
            html.Span("  ·  Phase 2 Advanced Analytics",
                      style={"fontSize": "13px", "color": COLOURS["subtext"],
                             "marginLeft": "6px"}),
        ]),
        html.Div("Data: stores.db · 2003–2005",
                 style={"fontSize": "11px", "color": COLOURS["subtext"]}),
    ]),

    # ── KPI strip
    html.Div(style={"display": "flex", "gap": "16px",
                    "padding": "24px 40px 0 40px"}, children=[
        kpi_box("Total Profit (all reps)", f"${total_profit_all}M"),
        kpi_box("Top Sales Rep",            top_rep,    COLOURS["gold"]),
        kpi_box("Top Office",               top_office, COLOURS["purple"]),
        kpi_box("Churn-Risk Customers",     str(num_churn), COLOURS["red"]),
        kpi_box("Platinum Profit Share",    f"{pct_platinum:.1f}%", COLOURS["gold"]),
    ]),

    # ── Tabs
    html.Div(style={"padding": "24px 40px"}, children=[
        dcc.Tabs(
            id="tabs",
            value="reps",
            style={"marginBottom": "20px"},
            colors={"border"    : COLOURS["border"],
                    "primary"   : COLOURS["purple"],
                    "background": COLOURS["bg"]},
            children=[

                # ════════════════════════════════════════
                # TAB 1 — Sales Reps & Offices
                # ════════════════════════════════════════
                dcc.Tab(label="👤  Sales Reps & Offices",
                        value="reps",
                        style={"backgroundColor": COLOURS["bg"],
                               "color": COLOURS["subtext"],
                               "borderColor": COLOURS["border"],
                               "padding": "10px 20px"},
                        selected_style={"backgroundColor": COLOURS["panel"],
                                        "color": COLOURS["text"],
                                        "borderTop": f"2px solid {COLOURS['purple']}",
                                        "padding": "10px 20px"},
                        children=[
                            html.Div(style={"display": "grid",
                                            "gridTemplateColumns": "1fr 1fr",
                                            "gap": "20px"}, children=[
                                card([
                                    section_title("Sales Rep Performance"),
                                    dcc.Graph(figure=fig_rep_profit(),
                                              config={"displayModeBar": False}),
                                ]),
                                card([
                                    section_title("Office Performance"),
                                    dcc.Graph(figure=fig_office_profit(),
                                              config={"displayModeBar": False}),
                                    html.Br(),
                                    section_title("Fulfilment Time by Status"),
                                    dcc.Graph(figure=fig_fulfilment(),
                                              config={"displayModeBar": False}),
                                ]),
                            ]),
                            card([
                                section_title("Management Hierarchy — Self-Join"),
                                styled_table(
                                    hierarchy,
                                    "tbl-hierarchy",
                                    col_fmt=[
                                        {"if": {"filter_query": '{job_title} = "President"'},
                                         "color": COLOURS["gold"], "fontWeight": "bold"},
                                        {"if": {"filter_query": '{job_title} contains "Manager"'},
                                         "color": COLOURS["purple"]},
                                    ]
                                ),
                            ]),
                        ]),

                # ════════════════════════════════════════
                # TAB 2 — Revenue Trends
                # ════════════════════════════════════════
                dcc.Tab(label="📈  Revenue Trends",
                        value="revenue",
                        style={"backgroundColor": COLOURS["bg"],
                               "color": COLOURS["subtext"],
                               "borderColor": COLOURS["border"],
                               "padding": "10px 20px"},
                        selected_style={"backgroundColor": COLOURS["panel"],
                                        "color": COLOURS["text"],
                                        "borderTop": f"2px solid {COLOURS['purple']}",
                                        "padding": "10px 20px"},
                        children=[
                            card([
                                section_title("Monthly Revenue — All Years"),
                                dcc.Graph(figure=fig_monthly_revenue(),
                                          config={"displayModeBar": False}),
                            ]),
                            html.Div(style={"display": "grid",
                                            "gridTemplateColumns": "1fr 1fr",
                                            "gap": "20px"}, children=[
                                card([
                                    section_title("Profit by Product Line — Annual"),
                                    dcc.Graph(figure=fig_product_line(),
                                              config={"displayModeBar": False}),
                                ]),
                                card([
                                    section_title("Monthly Detail Table"),
                                    styled_table(
                                        monthly[["year","month","num_orders",
                                                 "revenue","profit"]]
                                        .rename(columns={"year":"Year","month":"Month",
                                                          "num_orders":"Orders",
                                                          "revenue":"Revenue ($)",
                                                          "profit":"Profit ($)"}),
                                        "tbl-monthly"
                                    ),
                                ]),
                            ]),
                        ]),

                # ════════════════════════════════════════
                # TAB 3 — Customer Segmentation
                # ════════════════════════════════════════
                dcc.Tab(label="🎯  Customer Segments",
                        value="segments",
                        style={"backgroundColor": COLOURS["bg"],
                               "color": COLOURS["subtext"],
                               "borderColor": COLOURS["border"],
                               "padding": "10px 20px"},
                        selected_style={"backgroundColor": COLOURS["panel"],
                                        "color": COLOURS["text"],
                                        "borderTop": f"2px solid {COLOURS['purple']}",
                                        "padding": "10px 20px"},
                        children=[
                            html.Div(style={"display": "grid",
                                            "gridTemplateColumns": "1fr 1fr",
                                            "gap": "20px"}, children=[
                                card([
                                    section_title("Segment Distribution"),
                                    dcc.Graph(figure=fig_seg_donut(),
                                              config={"displayModeBar": False}),
                                ]),
                                card([
                                    section_title("Segment Total Profit"),
                                    dcc.Graph(figure=fig_seg_profit(),
                                              config={"displayModeBar": False}),
                                ]),
                            ]),
                            card([
                                section_title("All Customers — Segmented"),
                                dcc.Dropdown(
                                    id="seg-filter",
                                    options=[{"label": s, "value": s}
                                             for s in ["All","Platinum","Gold","Silver","Bronze"]],
                                    value="All",
                                    clearable=False,
                                    style={"backgroundColor": COLOURS["panel"],
                                           "color": "#000",
                                           "width": "200px",
                                           "marginBottom": "12px"},
                                ),
                                html.Div(id="seg-table"),
                            ]),
                            card([
                                section_title("⚠  Churn Risk — Dormant >180 Days"),
                                html.P(
                                    f"{num_churn} customers have not ordered in over 180 days "
                                    "and are at risk of churning. Bars are coloured by segment.",
                                    style={"color": COLOURS["subtext"],
                                           "fontSize": "12px", "margin": "0 0 12px 0"}
                                ),
                                html.Div(style={"display": "grid",
                                                "gridTemplateColumns": "1.2fr 1fr",
                                                "gap": "20px"}, children=[
                                    dcc.Graph(figure=fig_churn(),
                                              config={"displayModeBar": False}),
                                    styled_table(
                                        last_order[["customerName","country",
                                                    "segment","days_since",
                                                    "total_profit"]]
                                        .head(15)
                                        .rename(columns={
                                            "customerName":"Customer",
                                            "country":"Country",
                                            "segment":"Segment",
                                            "days_since":"Days Inactive",
                                            "total_profit":"Lifetime Profit ($)"
                                        }),
                                        "tbl-churn",
                                        col_fmt=[
                                            {"if": {"filter_query": '{Segment} = "Gold"'},
                                             "color": COLOURS["green"]},
                                            {"if": {"filter_query": '{Segment} = "Silver"'},
                                             "color": COLOURS["blue"]},
                                            {"if": {"filter_query": '{Segment} = "Platinum"'},
                                             "color": COLOURS["gold"]},
                                        ]
                                    ),
                                ]),
                            ]),
                        ]),
            ]),
    ]),
])


# ═══════════════════════════════════════════════════════════════
# 6.  CALLBACKS
# ═══════════════════════════════════════════════════════════════

@app.callback(Output("seg-table", "children"), Input("seg-filter", "value"))
def update_seg_table(seg_val):
    df = cust_profit if seg_val == "All" else cust_profit[cust_profit["segment"] == seg_val]
    display = df[["customerName","city","country",
                  "total_profit","total_orders","segment"]].copy()
    display.columns = ["Customer","City","Country",
                       "Profit ($)","Orders","Segment"]
    seg_colours = [
        {"if": {"filter_query": '{Segment} = "Platinum"'},
         "color": COLOURS["gold"], "fontWeight": "bold"},
        {"if": {"filter_query": '{Segment} = "Gold"'},    "color": COLOURS["green"]},
        {"if": {"filter_query": '{Segment} = "Silver"'},  "color": COLOURS["blue"]},
        {"if": {"filter_query": '{Segment} = "Bronze"'},  "color": COLOURS["red"]},
    ]
    return styled_table(display, "tbl-cust-detail", col_fmt=seg_colours)


# ═══════════════════════════════════════════════════════════════
# 7.  RUN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n  ┌─────────────────────────────────────────────┐")
    print("  │  Phase 2 Dashboard — Scale Model Car Store  │")
    print("  │  http://localhost:8050                       │")
    print("  └─────────────────────────────────────────────┘\n")
    app.run(debug=True, host="0.0.0.0", port=8050)
