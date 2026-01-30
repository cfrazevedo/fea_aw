with 
    int_product as (

        select *
        from {{ ref('int_product') }}

    )

    , final as (

        select
            product_sk
            , product_id
            , product_name
            , make_flag
            , product_style
            , case 
                when upper(trim(product_style)) = 'W' then 'Womens'
                when upper(trim(product_style)) = 'M' then 'Mens'
                when upper(trim(product_style)) = 'U' then 'Universal'
                else 'No style'
            end as style_name
            , case 
                when upper(trim(product_style)) = 'W' then 1
                when upper(trim(product_style)) = 'M' then 2
                when upper(trim(product_style)) = 'U' then 3
                else 4
            end as style_name_sort_by
            , coalesce(product_subcategory_id, '0') as product_subcategory_id -- todos os null são componentes (finished_goods_flag = 0)
            , coalesce(product_subcategory_name, 'Not salable') as product_subcategory_name
            , coalesce(product_category_id, '0') as product_category_id
            , coalesce(product_category_name, 'Not salable') as product_category_name
            , product_line
            , case 
                when upper(trim(product_line)) = 'R' then 'Road'
                when upper(trim(product_line)) = 'M' then 'Mountain'
                when upper(trim(product_line)) = 'T' then 'Touring'
                when upper(trim(product_line)) = 'S' then 'Standard'
                else 'No line'
            end as product_line_name
            , case 
                when upper(trim(product_line)) = 'R' then 1
                when upper(trim(product_line)) = 'M' then 2
                when upper(trim(product_line)) = 'T' then 3
                when upper(trim(product_line)) = 'S' then 4
                else 5
            end as product_line_name_sort_by
            , coalesce(product_model_id, '0') as product_model_id -- todos os null são componentes (finished_goods_flag = 0)
            , coalesce(product_model_name, 'Not salable') as product_model_name
            , standard_cost
            , unit_price
            , case
                when coalesce(unit_price, 0) > 0 and coalesce(standard_cost, 0) > 0 then
                    unit_price - standard_cost
                else 0
            end as profit
            , source_updated_at
            , current_timestamp() as updated_at
        
        from int_product
        
    )

select * from final