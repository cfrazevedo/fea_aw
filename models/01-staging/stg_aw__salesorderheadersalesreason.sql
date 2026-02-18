with 

    source as (

        select * from {{ source('sources_aw', 'sales_salesorderheadersalesreason') }}

    ),

    renamed as (

        select
            cast(salesorderid as int) as sales_order_id
            , cast(salesreasonid as int) as sales_reason_id
            , try_cast(modifieddate as timestamp) as modified_date        

        from source

    )

select * from renamed