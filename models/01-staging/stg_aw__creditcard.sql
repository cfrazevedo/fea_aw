with 

    source as (

        select * from {{ source('sources_aw', 'stg_aw__creditcard') }}

    ),

    renamed as (

        select
            cast(creditcardid as string) as credit_card_id
            , cast(cardtype as string) as card_type
            , cast(cardnumber as string) as card_number
            , cast(expmonth as string) as exp_month
            , cast(expyear as string) as exp_year
            , try_cast(modifieddate as timestamp) as modified_date

        from source

    )

select * from renamed
