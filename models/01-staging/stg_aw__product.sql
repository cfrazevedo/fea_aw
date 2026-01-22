with 

    source as (

        select * from {{ source('sources_aw', 'production_product') }}

    ),

    renamed as (

        select
            cast(productid as string) as product_id
            , {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk
            , cast(`name` as string) as product_name
            , cast(productnumber as string) as product_number
            , cast(makeflag as string) as make_flag
            , cast(finishedgoodsflag as string) as finished_goods_flag
            , cast(color as string) as color
            , cast(safetystocklevel as int) as safety_stock_level
            , cast(reorderpoint as int) as reorder_point
            , cast(standardcost as decimal(19,4)) as standard_cost
            , cast(listprice as decimal(19,4)) as list_price
            , cast(`size` as string) as `size`
            , cast(sizeunitmeasurecode as string) as size_unit_measure_code
            , cast(weightunitmeasurecode as string) as weight_unit_measure_code
            , cast(`weight` as decimal(8,2)) as `weight`
            , cast(daystomanufacture as int) as days_to_manufacture
            , cast(productline as string) as product_line
            , cast(class as string) as product_class
            , cast(style as string) as product_style
            , cast(productsubcategoryid as string) as product_subcategory_id
            , cast(productmodelid as string) as product_model_id
            , cast(sellstartdate as timestamp) as sell_start_date
            , cast(sellenddate as timestamp) as sell_end_date
            , cast(discontinueddate as timestamp) as discontinued_date
            , cast(rowguid as string) as rowguid
            , try_cast(modifieddate as timestamp) as modified_date

        from source

    )

select * from renamed