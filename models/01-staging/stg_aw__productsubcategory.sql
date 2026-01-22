with 

    source as (

        select * from {{ source('sources_aw', 'production_productsubcategory') }}

    ),

    renamed as (

        select
            cast(productsubcategoryid as string) as product_subcategory_id
            , cast(productcategoryid as string) as product_category_id
            , cast(`name` as string) as product_subcategory_name
            , cast(rowguid as string) as rowguid
            , cast(modifieddate as timestamp) as modified_date

        from source

    )

select * from renamed