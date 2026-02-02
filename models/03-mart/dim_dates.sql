with

    dim_dates as (

        {{ dbt_date.get_date_dimension("2000-01-01", "2030-12-31") }}

    )

    , final as (

        select *
            , cast(date_format(date_day, 'yyyyMM') as int) as year_month_sort
            , concat(month_name_short, '/', year_number) as year_month
            , cast(concat(year_number, quarter_of_year) as int) as year_quarter_sort
            , concat('Q', quarter_of_year, '/', year_number) as year_quarter

        from dim_dates
        
    )

select * from final