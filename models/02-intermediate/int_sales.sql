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

    , stg_address as (

        select *
        from {{ ref('stg_aw__address') }}

    )

    , stg_stateprovince as (

        select *
        from {{ ref('stg_aw__stateprovince') }}

    )

    , stg_creditcard as (

        select *
        from {{ ref('stg_aw__creditcard') }}

    )

    , join_address as (

        select
            stg_address.address_id
            , stg_address.city_name
            , stg_stateprovince.state_province_name
        
        from stg_address
        left join stg_stateprovince
        on stg_address = stg_stateprovince.state_province_id

    )

    , join_detail as (

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

    , join_header as (

        select
            stg_header.sales_order_id
            , stg_header.customer_id
            , stg_header.sales_person_id
            , stg_header.territory_id
            , stg_header.order_date
            , stg_header.due_date
            , stg_header.ship_date
            , stg_header.`status`
            , stg_header.ship_to_address_id
            , join_address.city_name
            , join_address.state_province_name
            , stg_header.credit_card_id
            , stg_creditcard.card_type
            , stg_header.tax_amount
            , stg_header.freight

        from stg_header
        left join join_address
        on stg_header.ship_to_address_id = join_address.address_id
        left join stg_creditcard
        on stg_header.credit_card_id = stg_creditcard.credit_card_id

    )
    
    , join_detail_header as (

        select
            join_detail.sales_order_detail_id
            , join_detail.sales_order_id
            , join_detail.product_id
            , join_header.customer_id
            , join_header.sales_person_id
            , join_header.territory_id
            , join_header.order_date
            , join_header.due_date
            , join_header.ship_date
            , join_detail.order_quantity
            , join_detail.unit_price
            , case
                when join_detail.unit_price_discount != 0
                    then join_detail.unit_price_discount
                else null
            end as unit_price_discount_percentage
            , round(join_detail.unit_price_discount * join_detail.unit_price * join_detail.order_quantity, 3) as unit_price_discount_value 
            , join_detail.line_total
            , join_detail.standard_cost
            , join_header.tax_amount
            , join_header.freight
            , join_header.`status`
            , join_header.city_name
            , join_header.state_province_name
            , join_header.card_type
            , join_detail.source_updated_at

        from join_detail
        left join join_header
        on join_detail.sales_order_id = join_header.sales_order_id

    )

    , generate_sk as (

        select
            {{ dbt_utils.generate_surrogate_key(['join_detail_header.sales_order_detail_id']) }} as sales_sk
            , {{ dbt_utils.generate_surrogate_key(['join_detail_header.product_id']) }} as product_fk
            , {{ dbt_utils.generate_surrogate_key(['join_detail_header.customer_id']) }} as customer_fk
            , *
            , (line_total / sum(line_total) over(partition by sales_order_id)) * tax_amount as tax_amount_portion
            , (line_total / sum(line_total) over(partition by sales_order_id)) * freight as freight_portion

        from join_detail_header
        
    )

select * from generate_sk