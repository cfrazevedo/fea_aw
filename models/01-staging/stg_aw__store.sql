with 

    source as (

        select * from {{ source('sources_aw', 'sales_store') }}

    ),

    renamed as (

        select
            cast(businessentityid as int) as store_id
            , cast(`name` as string) as store_name
            , cast(salespersonid as int) as sales_person_id
            , cast(demographics as string) as demographics
            , cast(rowguid as string) as rowguid
            , try_cast(modifieddate as timestamp) as modified_date

        from source

    )

select * from renamed