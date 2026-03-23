from textwrap import dedent
from dataclasses import dataclass

@dataclass
class AggregateQueries:
    table_name: str
    query: str
   
agg_monthly_billing = AggregateQueries(
    table_name="agg_monthly_billing",
    query=dedent("""
        SELECT
            strftime(d.full_date, '%Y-%m') AS year_month,
            SUM(f.gross_amount) AS total_billing,
            COUNT(*) AS total_orders
        FROM delta_scan('{gold_path}/fact_sales') f
        JOIN delta_scan('{gold_path}/dim_date') d
            ON f.date_key = d.date_key
        WHERE f.is_shipped = TRUE
        GROUP BY year_month
        ORDER BY year_month
    """)
)

agg_sla = AggregateQueries(
    table_name="agg_sla",
    query=dedent("""
        SELECT
            strftime(d.full_date, '%Y-%m') AS year_month,
            COUNT(*) AS total_orders,
            SUM(CASE WHEN f.sla_met THEN 1 ELSE 0 END) AS orders_on_time,
            AVG(CASE WHEN f.sla_met THEN 1 ELSE 0 END) AS sla_rate,
            AVG(f.delay_days) AS avg_delay_days
        FROM delta_scan('{gold_path}/fact_sales') f
        JOIN delta_scan('{gold_path}/dim_date') d
            ON f.date_key = d.date_key
        WHERE f.is_shipped = TRUE
        GROUP BY year_month
        ORDER BY year_month
    """)
)

agg_delay = AggregateQueries(
    table_name="agg_delay",
    query=dedent("""
        SELECT
            strftime(d.full_date, '%Y-%m') AS year_month,
            COUNT(*) AS total_orders,
            SUM(CASE WHEN f.is_late THEN 1 ELSE 0 END) AS total_late_orders,
            AVG(CASE WHEN f.is_late THEN 1 ELSE 0 END) AS late_rate,
            AVG(CASE WHEN f.is_late THEN f.delay_days END) AS avg_delay_days,
        FROM delta_scan('{gold_path}/fact_sales') f
        JOIN delta_scan('{gold_path}/dim_date') d
            ON f.date_key = d.date_key
        WHERE f.is_shipped = TRUE
        GROUP BY year_month
        ORDER by year_month
    """)
)

agg_top_products = AggregateQueries(
    table_name="agg_top_products",
    query=dedent("""
        SELECT
            p.product_id,
            p.product_name,            
            SUM(f.quantity) AS total_quantity,
            SUM(f.gross_amount) AS total_revenue,            
            RANK() OVER (ORDER BY SUM(f.gross_amount) DESC) AS revenue_rank,            
            SUM(f.gross_amount) * 1.0
                / SUM(SUM(f.gross_amount)) OVER () AS revenue_share
        FROM delta_scan('{gold_path}/fact_sales') f
        JOIN delta_scan('{gold_path}/dim_product') p
            ON f.product_id = p.product_id
        WHERE f.is_shipped = TRUE
        GROUP BY
            p.product_id,
            p.product_name
        ORDER BY total_revenue DESC
    """)
)

agg_monthly_average_ticket = AggregateQueries(
    table_name="agg_monthly_average_ticket",
    query=dedent("""
        SELECT
            strftime(d.full_date, '%Y-%m') AS year_month,
            SUM(f.gross_amount) AS total_revenue,
            COUNT(*) AS total_orders,
            SUM(f.gross_amount) * 1.0 / COUNT(*) AS avg_ticket
        FROM delta_scan('{gold_path}/fact_sales') f
        JOIN delta_scan('{gold_path}/dim_date') d
            ON f.date_key = d.date_key
        WHERE f.is_shipped = TRUE
        GROUP BY year_month
        ORDER BY year_month
    """)
)

agg_monthly_discount = AggregateQueries(
    table_name="agg_monthly_discount",
    query=dedent("""
        SELECT
            strftime(d.full_date, '%Y-%m') AS year_month,
            SUM(f.gross_amount) AS total_gross,
            SUM(f.discount * f.gross_amount) AS total_discount_value,
            SUM(f.discount * f.gross_amount) * 1.0
                / SUM(f.gross_amount) AS discount_rate
        FROM delta_scan('{gold_path}/fact_sales') f
        JOIN delta_scan('{gold_path}/dim_date') d
            ON f.date_key = d.date_key
        WHERE f.is_shipped = TRUE
        GROUP BY year_month
        ORDER BY year_month
    """)
)

AGG_CONFIGS = [
    agg_monthly_billing,
    agg_sla,
    agg_delay,
    agg_top_products,
    agg_monthly_average_ticket,
    agg_monthly_discount
]
