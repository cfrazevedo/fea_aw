with

    stg_header as (

        select *
        from {{ ref("stg_aw__salesorderheader") }}

    )

    , stg_detail as (

        select *
        from {{ ref('stg_aw__salesorderdetail') }}
        
    )

    , stg_product as (

        select *
        from {{ ref('stg_aw__product') }}

    )

    , join_detail_product as (

        select
            stg_detail.sales_order_id
            , stg_detail.sales_order_detail_id
            , stg_detail.order_quantity
            , stg_detail.product_id
            , stg_detail.unit_price
            , stg_detail.unit_price_discount
            , stg_product.standard_cost
            , stg_detail.unit_price * (1 - stg_detail.unit_price_discount) * stg_detail.order_quantity as line_total
            , stg_detail.modified_date as source_updated_at

        from stg_detail
        left join stg_product
        on stg_detail.product_id = stg_product.product_id

    )

    , join_detail_header as (

        select
            join_detail_product.sales_order_detail_id
            , join_detail_product.sales_order_id
            , join_detail_product.product_id
            , stg_header.customer_id
            , stg_header.sales_person_id
            , stg_header.territory_id
            , stg_header.order_date
            , stg_header.due_date
            , stg_header.ship_date
            , join_detail_product.order_quantity
            , join_detail_product.unit_price
            , case
                when join_detail_product.unit_price_discount != 0
                    then join_detail_product.unit_price_discount
                else null
            end as unit_price_discount_percentage
            , round(join_detail_product.unit_price_discount * join_detail_product.unit_price * join_detail_product.order_quantity, 3) as unit_price_discount_value 
            , join_detail_product.standard_cost
            , stg_header.`status`
            , join_detail_product.source_updated_at

        from join_detail_product
        left join stg_header
        on join_detail_product.sales_order_id = stg_header.sales_order_id

    )

    , generate_sk as (

        select
            {{ dbt_utils.generate_surrogate_key(['join_detail_header.sales_order_detail_id']) }} as sales_sk
            , {{ dbt_utils.generate_surrogate_key(['join_detail_header.product_id']) }} as product_fk
            , {{ dbt_utils.generate_surrogate_key(['join_detail_header.customer_id']) }} as customer_fk
            , *
        from join_detail_header
        
    )

select * from generate_sk