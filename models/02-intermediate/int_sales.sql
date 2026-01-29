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
            stg_detail.sales_sk
            , stg_detail.sales_order_fk
            , stg_detail.product_fk
            , stg_detail.sales_order_id
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
        on stg_detail.product_fk = stg_product.product_sk

    )

    , join_detail_header as (

        select
            join_detail_product.sales_sk
            , join_detail_product.sales_order_fk
            , join_detail_product.product_fk
            , header.customer_fk
            , join_detail_product.sales_order_detail_id
            , join_detail_product.sales_order_id
            , join_detail_product.product_id
            , header.customer_id
            , header.sales_person_id
            , header.territory_id
            , header.order_date
            , header.due_date
            , header.ship_date
            , join_detail_product.order_quantity
            , join_detail_product.unit_price
            , case
                when join_detail_product.unit_price_discount != 0
                    then join_detail_product.unit_price_discount
                else null
            end as unit_price_discount_percentage
            , round(join_detail_product.unit_price_discount * join_detail_product.unit_price * join_detail_product.order_quantity, 3) as unit_price_discount_value 
            , join_detail_product.standard_cost
            , header.`status`
            , join_detail_product.source_updated_at

        from join_detail_product
        left join header
        on join_detail_product.sales_order_fk = header.sales_order_sk

    )

select * from join_detail_header