with

    sales as (

        select * 
        from {{ ref('int_sales') }}

    )

    , first_orders as (

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
                when first_orders.first_order = sales.order_date
                    then 'true'
                else 'false'
            end as is_first_order
        
        from sales
        left join first_orders
        on sales.customer_id = first_orders.customer_id

    )

    , final as (

        select
            sales_sk
            , sales_order_detail_id
            , sales_order_id
            , product_fk
            , customer_fk
            , {{ dbt_utils.generate_surrogate_key(['sales_person_id', 'territory_id']) }} as sales_hierarchy_fk
            , sales_person_id
            , territory_id
            , city_name
            , state_province_name
            , order_quantity
            , unit_price
            , unit_price_discount_percentage
            , unit_price_discount_value
            , line_total
            , tax_amount_portion
            , freight_portion
            , line_total + tax_amount_portion + freight_portion as subtotal_portion
            , standard_cost
            , line_total - (standard_cost * order_quantity) as profit
            , case
                when card_type is null
                    then "Another payment method"
                else card_type
            end as card_type
            , order_date
            , due_date
            , ship_date
            , is_first_order
            , case
                when is_first_order = TRUE then 'new'
                else 'current'
            end as customer_type
            , `status`
            , source_updated_at
            , current_timestamp() as updated_at
        
        from join_sales
        where `status` = 5
        
    )

select * from final