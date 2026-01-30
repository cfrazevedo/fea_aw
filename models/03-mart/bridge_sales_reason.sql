with 

    int_salesreason as (

        select * 
        from {{ ref('int_salesreason') }}

    )

    , int_sales as (

        select *
        from {{ ref('int_sales') }}

    )

    , join_tables as (

        select
            int_sales.sales_sk as sales_fk
            , int_salesreason.sales_order_fk
            , int_salesreason.sales_reason_sk as sales_reason_fk
        from int_sales
        full join int_salesreason
        on int_sales.sales_order_fk = int_salesreason.sales_order_fk
        where int_salesreason.sales_reason_sk is not null

    )

    , final as (

        select
            {{ dbt_utils.generate_surrogate_key(['sales_order_fk', 'sales_reason_fk', 'sales_fk']) }} as bridge_sales_reason_sk
            , sales_fk
            , sales_reason_fk
            , current_timestamp() as updated_at
        from join_tables

    )

select * from final