with 

    customer as (

        select * 
        from {{ ref('int_customer') }}

    )

    , base as (

        select
            *
            , customer_id
            , customer_name
            , count(*) over (partition by customer_name) as name_count
        
        from customer

    )

    , final as (

        select
            customer_sk
            , customer_id
            , person_id
            , customer_name
            , case
                when name_count > 1 then
                    concat(customer_name, ' (', cast(customer_id as string), ')')
                else
                    customer_name
            end as customer_display
            , store_id
            , store_name
            , territory_id
            , source_updated_at
            , current_timestamp() as updated_at
        
        from base
        
    )

select * from final