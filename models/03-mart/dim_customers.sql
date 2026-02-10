with 

    customer as (

        select * 
        from {{ ref('int_customer') }}

    )

    , final as (

        select
            customer_sk
            , customer_id
            , person_id
            , customer_name
            , concat(store_name, ' (', customer_name, ')') as customer_display
            , store_id
            , store_name
            , territory_id
            , source_updated_at
            , current_timestamp() as updated_at
        
        from customer
        
    )

select * from final