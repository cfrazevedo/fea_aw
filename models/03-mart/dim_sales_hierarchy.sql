with 

    int_sales as (

        select distinct
            sales_person_id
            , territory_id
        from {{ ref('int_sales') }}

    )

    , int_salesterritory as (

        select * 
        from {{ ref('int_salesterritory')}}

    )

    , int_salesperson as (

        select
            sales_person_id
            , concat(first_name, ' ', last_name) as sales_person_name
            , person_type
            , sales_quota
            , bonus
            , commission_pct
            , sales_ytd
            , sales_last_year
            , source_updated_at

        from {{ ref('int_salesperson') }}

    )

    , join_salesperson as (

        select
            int_sales.sales_person_id
            , int_sales.territory_id
            , int_salesperson.sales_person_name
            , int_salesperson.person_type
            , int_salesperson.sales_quota
            , int_salesperson.bonus
            , int_salesperson.commission_pct
            , int_salesperson.sales_ytd
            , int_salesperson.sales_last_year
            , int_salesperson.source_updated_at
        
        from int_sales
        left join int_salesperson
        on int_sales.sales_person_id = int_salesperson.sales_person_id

    )

    , join_saleshierarchy as (

        select
            join_salesperson.sales_person_id
            , COALESCE(join_salesperson.sales_person_name, 'Online') as sales_person_name
            , join_salesperson.person_type
            , join_salesperson.sales_quota
            , join_salesperson.bonus
            , join_salesperson.commission_pct
            , join_salesperson.sales_ytd
            , join_salesperson.sales_last_year
            , join_salesperson.territory_id
            , join_salesperson.source_updated_at
            , int_salesterritory.territory_name
            , int_salesterritory.country_region_code
            , int_salesterritory.territory_group
        
        from join_salesperson
        left join int_salesterritory
        on join_salesperson.territory_id = int_salesterritory.territory_id

    )

    , final as (

        select
            {{ dbt_utils.generate_surrogate_key(['sales_person_id', 'territory_id']) }} as sales_hierarchy_sk
            , sales_person_id
            , sales_person_name
            , person_type
            , sales_quota
            , bonus
            , commission_pct
            , sales_ytd
            , sales_last_year
            , territory_id
            , territory_name
            , country_region_code
            , case 
                when upper(trim(country_region_code)) = 'US' then 'USA'
                when upper(trim(country_region_code)) = 'CA' then 'Canada'
                when upper(trim(country_region_code)) = 'FR' then 'France'
                when upper(trim(country_region_code)) = 'DE' then 'Germany'
                when upper(trim(country_region_code)) = 'AU' then 'Australia'
                when upper(trim(country_region_code)) = 'GB' then 'United Kingdom'
                else 'Unknown'
            end as country
            , territory_group
            , source_updated_at
            , current_timestamp() as updated_at
        from join_saleshierarchy
        
    )

select * from final