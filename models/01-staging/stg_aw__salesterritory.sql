with 

    source as (

        select * from {{ source('sources_aw', 'sales_salesterritory') }}

    ),

    renamed as (

        select
            cast(territoryid as string) as territory_id
            , cast(`name` as string) as territory_name
            , cast(countryregioncode as string) as country_region_code
            , cast(group as string) as territory_group
            , cast(salesytd as decimal(19,4)) as sales_ytd
            , cast(saleslastyear as decimal(19,4)) as sales_last_year
            , cast(costytd as decimal(19,4)) as cost_ytd
            , cast(costlastyear as decimal(19,4)) as cost_last_year
            , cast(rowguid as string) as rowguid
            , cast(modifieddate as string) as modified_date

        from source

    )

select * from renamed