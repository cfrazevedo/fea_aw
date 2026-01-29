with 

    source as (

        select * from {{ source('sources_aw', 'sales_salesreason') }}

    ),

    renamed as (

        select
            cast(salesreasonid as string) as sales_reason_id
            , cast(`name` as string) as sales_reason_name
            , cast(reasontype as string) as sales_reason_type
            , try_cast(modifieddate as timestamp) as modified_date

        from source

    )

select * from renamed