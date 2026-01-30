with

    int_salesreason as (

        select * from {{ ref('int_salesreason') }}

    )

    , final as (

        select
            sales_reason_sk
            , sales_order_id
            , sales_reason_id
            , sales_reason_name
            , sales_reason_type
            , source_updated_at
            , current_timestamp() as updated_at
        
        from int_salesreason
        
    )

select * from final