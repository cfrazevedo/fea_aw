with 

    customer as (

        select * 
        from {{ ref('int_customer') }}

    )

    , stg_store as (

        select *
        from {{ ref('stg_aw__store' ) }}

    )

    , final as (

        select
            customer.customer_sk
            , customer.customer_id
            , customer.person_id
            , customer.customer_name
            , customer.store_id
            , stg_store.store_name
            , customer.territory_id
            , customer.source_updated_at
            , current_timestamp() as updated_at
        
        from customer
        left join stg_store
        on customer.store_id = stg_store.store_id
        
    )

select * from final