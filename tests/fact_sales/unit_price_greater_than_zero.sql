select
    sales_sk
    , due_date
    , order_date
    , ship_date
from {{ ref('fact_sales') }}
where
    order_date > ship_date
    or ship_date > due_date