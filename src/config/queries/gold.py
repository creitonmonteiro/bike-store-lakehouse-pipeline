from textwrap import dedent
from dataclasses import dataclass

@dataclass
class GoldQueryConfig:
    dim_customer: str = dedent("""
        SELECT DISTINCT
            customer_id,
            first_name,
            last_name,
            email,
            city,
            state
        FROM silver.customers
    """)

    dim_staff: str = dedent("""
        SELECT DISTINCT
            staff_id,
            store_id,
            first_name,
            last_name,
            active
        FROM silver.staffs
    """)

    dim_store: str = dedent("""
        SELECT DISTINCT
            store_id,
            store_name
        FROM silver.stores
    """)

    dim_product: str = dedent("""
        SELECT DISTINCT
            p.product_id,
            p.product_name,
            p.model_year,
            p.list_price,
            p.category_id,
            c.category_name,
            p.brand_id,
            b.brand_name
        FROM silver.products p
                      
        JOIN silver.categories c
          ON p.category_id = c.category_id
                      
        LEFT JOIN silver.brands b
          ON p.brand_id = b.brand_id
                      
        WHERE c.category_id IS NOT NULL
    """)

    fact_sales: str = dedent("""
        SELECT
            oi.order_id,
            o.order_date,
            oi.product_id,

            d.date_key,
            c.customer_key,
            c.first_name || ' ' || c.last_name AS customer_name,
                        
            s.store_key,
            s.store_name,
                    
            stf.staff_key,
            (stf.first_name || ' ' || stf.last_name) AS staff_name,

            oi.quantity,
            oi.list_price as unit_price,
            (oi.quantity * oi.list_price) AS total_item_amount,
            oi.discount,
            oi.gross_amount,
            oi.net_amount
            {orders_derived_columns}

        FROM silver.order_items oi

        JOIN silver.orders o
            ON oi.order_id = o.order_id

        LEFT JOIN gold.dim_date d
            ON o.order_date = d.full_date

        LEFT JOIN gold.dim_customer c
            ON o.customer_id = c.customer_key
                        
        LEFT JOIN gold.dim_store s
            ON o.store_id = s.store_key
                    
        LEFT JOIN gold.dim_staff stf
            ON o.staff_id = stf.staff_key

        WHERE d.date_key IS NOT NULL
        AND c.customer_key IS NOT NULL
        AND o.order_date >= DATE '{last_date}' - INTERVAL '7 days'
    """)