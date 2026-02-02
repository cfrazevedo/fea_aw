select
    sales_sk,
    order_quantity
from {{ ref('fact_sales') }}
where order_quantity <= 0
    or unit_price <= 0