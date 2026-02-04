with 

    source as (

        select * from {{ source('sources_aw', 'person_stateprovince') }}

    ),

    renamed as (

        select
            cast(stateprovinceid as int) as state_province_id
            , cast(stateprovincecode as string) as state_province_code
            , cast(countryregioncode as string) as countryregioncode
            , cast(isonlystateprovinceflag as string) as is_only_state_province_flag
            , cast(`name` as string) as state_province_name
            , cast(territoryid as int) as territory_id
            , cast(rowguid as string) as rowguid
            , try_cast(modifieddate as timestamp) as modified_date

        from source

    )

select * from renamed
