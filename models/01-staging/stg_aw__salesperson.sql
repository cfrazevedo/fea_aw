with 

    source as (

        select * from {{ source('sources_aw', 'sales_salesperson') }}

    ),

    renamed as (

        select
            cast(businessentityid as string) as sales_person_id
            , cast(territoryid as string) as territory_id
            , cast(salesquota as decimal(19,4)) as sales_quota
            , cast(bonus as decimal(19,4)) as bonus
            , cast(commissionpct as decimal(19,4)) as commission_pct
            , cast(salesytd as decimal(19,4)) as sales_ytd
            , cast(saleslastyear as decimal(19,4)) as sales_last_year
            , cast(rowguid as string) as rowguid
            , cast(modifieddate as string) as modified_date            

        from source

    )

select * from renamed