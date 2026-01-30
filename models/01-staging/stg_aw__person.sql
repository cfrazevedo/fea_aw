with 

    source as (

        select * from {{ source('sources_aw', 'person_person') }}

    ),

    renamed as (

        select
            cast(businessentityid as string) as person_id
            , cast(persontype as string) as person_type
            , cast(namestyle as string) as name_style
            , cast(title as string) as title
            , cast(firstname as string) as first_name
            , cast(middlename as string) as middle_name
            , cast(lastname as string) as last_name
            , cast(suffix as string) as suffix
            , cast(emailpromotion as string) as email_promotion
            , cast(additionalcontactinfo as string) as additional_contact_info
            , cast(demographics as string) as demographics
            , try_cast(rowguid as string) as rowguid
            , try_cast(modifieddate as timestamp) as modified_date

        from source

    )

select *
from renamed