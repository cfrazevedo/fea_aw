with

    stg_product as (

        select *
        from {{ ref('stg_aw__product') }}

    )

    , stg_productcategory as (

        select *
        from {{ ref('stg_aw__productcategory') }}

    )

    , stg_productsubcategory as (

        select *
        from {{ ref('stg_aw__productsubcategory') }}

    )

    , stg_productmodel as (

        select *
        from {{ ref('stg_aw__productmodel') }}

    )

    , join_productcategory as (

        select
            stg_productsubcategory.product_subcategory_id
            , stg_productsubcategory.product_subcategory_name
            , stg_productsubcategory.product_category_id
            , stg_productcategory.product_category_name
        
        from stg_productsubcategory
        left join stg_productcategory
        on stg_productsubcategory.product_category_id = stg_productcategory.product_category_id

    )

    , join_product_model as (

        select
            stg_product.product_sk
            , stg_product.product_id
            , stg_product.product_name
            , stg_product.product_number
            , stg_product.make_flag
            , stg_product.finished_goods_flag
            , stg_product.color
            , stg_product.standard_cost
            , stg_product.list_price as unit_price
            , stg_product.product_line
            , stg_product.product_class
            , stg_product.product_style
            , stg_product.product_subcategory_id
            , stg_product.product_model_id
            , stg_productmodel.product_model_name
            , stg_product.sell_start_date
            , stg_product.sell_end_date
            , stg_product.discontinued_date
            , stg_product.modified_date
        
        from stg_product
        left join stg_productmodel
        on stg_product.product_model_id = stg_productmodel.product_model_id

    )

    , join_product as (

        select
            join_product_model.product_sk
            , join_product_model.product_id
            , join_product_model.product_name
            , join_product_model.product_number
            , join_product_model.make_flag
            , join_product_model.finished_goods_flag
            , join_product_model.color
            , join_product_model.standard_cost
            , join_product_model.unit_price
            , join_product_model.product_line
            , join_product_model.product_class
            , join_product_model.product_style
            , join_product_model.product_subcategory_id
            , join_product_modelcategory.product_subcategory_name
            , join_product_modelcategory.product_category_id
            , join_product_modelcategory.product_category_name
            , join_product_model.product_model_id
            , join_product_model.product_model_name
            , join_product_model.sell_start_date
            , join_product_model.sell_end_date
            , join_product_model.discontinued_date
            , join_product_model.modified_date as source_updated_at
        
        from join_product_model
        left join join_productcategory
        on join_product_model.product_subcategory_id = join_productcategory.product_subcategory_id
        
    )

select * from join_product