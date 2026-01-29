with

    stg_salesterritory as (

        select *
        from {{ ref('stg_aw__salesterritory' ) }}

    )

    , selected as (

        select
            territory_id
            , territory_name
            , country_region_code
            , territory_group
            , sales_ytd
            , sales_last_year
            , cost_ytd
            , cost_last_year
            , modified_date as source_updated_at

        from stg_salesterritory
        
    )

select * from selected