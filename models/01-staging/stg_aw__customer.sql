with 

    source as (

        select * from {{ source('sources_aw', 'sales_customer') }}

    ),

    renamed as (

        select
            cast(customerid as string) as customer_id
            , {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk
            , cast(personid as string) as person_id
            , cast(storeid as string) as store_id
            , cast(territoryid as string) as territory_id
            , cast(rowguid as string) as rowguid
            , try_cast(modifieddate as timestamp) as modified_date

        from source

    )

select * from renamed