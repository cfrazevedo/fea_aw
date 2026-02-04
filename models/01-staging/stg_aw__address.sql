with 

    source as (

        select * from {{ source('sources_aw', 'person_address') }}

    ),

    renamed as (

        select
            cast(addressid as int) as address_id
            , cast(addressline1 as string) as address_line_1
            , cast(addressline2 as string) as address_line_2
            , cast(city as string) as city_name
            , cast(stateprovinceid as int) as state_province_id
            , cast(postalcode as string) as postal_code
            , cast(spatiallocation as string) as spatial_location
            , cast(rowguid as string) as rowguid
            , try_cast(modifieddate as timestamp) as modified_date

        from source

    )

select * from renamed
