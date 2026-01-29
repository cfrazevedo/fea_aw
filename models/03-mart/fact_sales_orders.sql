with

    sales as (

        select * 
        from {{ ref('int_sales') }}

    )

    , first_order as (

        select 
            customer_id
            , cast(min(order_date) as timestamp) as first_order
        from sales
        group by customer_id

    )

    , join_sales as (

        select
            sales.*
            , case
                when first_order.first_order = sales.order_date
                    then 'true'
                else 'false'
            end as is_first_order
        
        from sales
        left join first_order
        on join_sales_order.customer_id = first_order.customer_id

    )

    , final as (

        select
            sales_sk
            , sales_order_fk
            , product_fk
            , customer_fk
            , {{ dbt_utils.generate_surrogate_key(['sales_person_id', 'territory_id']) }} as sales_hierarchy_fk
            , sales_person_id
            , territory_id
            , order_quantity
            , unit_price
            , unit_price_discount_percentage
            , unit_price_discount_value
            , (order_quantity * unit_price) - unit_price_discount_value as total_net_sales_detail
            , standard_cost
            , total_net_sales_detail - standard_cost as profit
            , order_date
            , due_date
            , ship_date
            , is_first_order
            , case
                when is_first_order = TRUE then 'new'
                else 'current'
            end as customer_type
            , source_updated_at
            , current_timestamp() as updated_at
        
        from join_sales
        where `status` = 5
        
    )

select * from final