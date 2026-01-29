with 

    source as (

        select * from {{ source('sources_aw', 'production_productmodel') }}

    ),

    renamed as (

        select
            cast(productmodelid as string) as product_model_id
            , cast(`name` as string) as product_model_name
            , cast(catalogdescription as string) as catalog_description
            , cast(instructions as string) as instructions
            , cast(rowguid as string) as rowguid
            , try_cast(modifieddate as timestamp) as modified_date

        from source

    )

select * from renamed