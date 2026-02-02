with 

    customer as (

        select * 
        from {{ ref('int_customer') }}

    )

    , salesperson as (

        select
            sales_person_id
            , concat(first_name, ' ', last_name) as sales_person_name

        from {{ ref('int_salesperson') }}

    )

    , final as (

        select
            customer.customer_sk
            , customer.customer_id
            , customer.person_id
            , salesperson.sales_person_name as customer_name
            , customer.store_id
            , customer.store_name
            , customer.territory_id
            , customer.source_updated_at
            , current_timestamp() as updated_at
        
        from customer
        left join salesperson
        on customer.person_id = salesperson.sales_person_id 
        
    )

select * from final