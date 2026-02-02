with

    stg_customer as (

        select *
        from {{ ref('stg_aw__customer' ) }}

    )

    , stg_store as (

        select *
        from {{ ref('stg_aw__store' ) }}

    )

    , join_customer as (

        select
            stg_customer.customer_id
            , stg_customer.person_id
            , stg_customer.store_id
            , stg_store.store_name
            , stg_customer.territory_id
            , stg_customer.modified_date as source_updated_at

        from stg_customer
        left join stg_store
        on stg_customer.store_id = stg_store.store_id
        
    )

    generate_sk as (

        select
            {{ dbt_utils.generate_surrogate_key(['join_customer.customer_id']) }} as customer_sk
            , *
        from join_customer
        
    )

select * from generate_sk