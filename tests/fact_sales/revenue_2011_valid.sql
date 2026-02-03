with 

    revenue as (
        
        select
            sum(line_total) as revenue_2011
            , 12646112.16 as goal
            , 0.001 as percentage_limit
        from {{ ref('fact_sales') }}
        where year(order_date) = 2011
    )

    , verification as (

        select
            revenue_2011
            , goal
            , (revenue_2011 / goal) as revenue_percentage

        from revenue
        where revenue_2011 < (goal * (1 - percentage_limit))
        or revenue_2011 > (goal * (1 + percentage_limit))
    )

    select * from verification
