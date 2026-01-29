with

    stg_salesperson as (

        select *
        from {{ ref('stg_aw__salesperson' ) }}

    )

    , stg_person as (

        select *
        from {{ ref('stg_aw__person' ) }}

    )

    , int_salesperson as (

        select
            stg_salesperson.sales_person_id
            , stg_person.person_type
            , stg_person.first_name
            , stg_person.last_name
            , stg_salesperson.sales_quota
            , stg_salesperson.bonus
            , stg_salesperson.commission_pct
            , stg_salesperson.sales_ytd
            , stg_salesperson.sales_last_year
            , stg_salesperson.modified_date as source_updated_at

        from stg_salesperson
        left join stg_person
        on stg_salesperson.sales_person_id = stg_person.person_id
        
    )

select * from int_salesperson