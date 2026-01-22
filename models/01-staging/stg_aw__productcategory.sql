with 

    source as (

        select * from {{ source('sources_aw', 'production_productcategory') }}

    ),

    renamed as (

        select
            cast(productcategoryid as string) as product_category_id
            , cast(`name` as string) as product_category_name
            , cast(rowguid as string) as rowguid
            , try_cast(modifieddate as timestamp) as modified_date

        from source

    )

select * from renamed