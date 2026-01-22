with 

    source as (

        select * from {{ source('sources_aw', 'sales_salesorderheader') }}

    ),

    renamed as (

        select
            cast(salesorderid as string) as sales_order_id
            , {{ dbt_utils.generate_surrogate_key(['sales_order_id']) }} as sales_order_sk
            , cast(revisionnumber as int) as revision_number
            , try_cast(orderdate as timestamp) as order_date
            , try_cast(duedate as timestamp) as due_date
            , try_cast(shipdate as timestamp) as ship_date
            , cast(`status` as string) as `status`
            , cast(onlineorderflag as boolean) as online_order_flag
            , cast(purchaseordernumber as string) as purchase_order_number
            , cast(accountnumber as string) as account_number
            , cast(customerid as string) as customer_id
            , {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_fk
            , cast(salespersonid as string) as sales_person_id
            , cast(territoryid as string) as territory_id
            , cast(billtoaddressid as string) as bill_to_address_id
            , cast(shiptoaddressid as string) as ship_to_address_id
            , cast(shipmethodid as string) as ship_method_id
            , cast(creditcardid as string) as credit_card_id
            , cast(creditcardapprovalcode as string) as credit_card_approval_code
            , cast(currencyrateid as string) as currency_rate_id
            , cast(subtotal as float) as sub_total
            , cast(taxamt as float) as tax_amount
            , cast(freight as float) as freight
            , cast(totaldue as float) as total_due
            , cast(comment as string) as comment
            , cast(rowguid as string) as rowguid
            , try_cast(modifieddate as timestamp) as modified_date

        from source

    )

select * from renamed