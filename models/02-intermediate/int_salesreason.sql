with

    stg_salesorderheadersalesreason as (

        select *
        from {{ ref('stg_aw__salesorderheadersalesreason') }}

    )
    
    , stg_salesreason as (

        select *
        from {{ ref('stg_aw__salesreason' ) }}

    )

    , join_salesreason as (

        select 
            stg_salesorderheadersalesreason.sales_order_id
            , stg_salesorderheadersalesreason.sales_reason_id
            , stg_salesreason.sales_reason_name
            , stg_salesreason.sales_reason_type
            , stg_salesorderheadersalesreason.modified_date as source_updated_at
        
        from stg_salesorderheadersalesreason
        left join stg_salesreason
        on stg_salesorderheadersalesreason.sales_reason_id = stg_salesreason.sales_reason_id

    )

    , generate_sk as (

        select 
            *
            , {{ dbt_utils.generate_surrogate_key(['sales_order_id', 'sales_reason_id']) }} as sales_reason_sk
            
        from join_salesreason
        
    )

select * from generate_sk 