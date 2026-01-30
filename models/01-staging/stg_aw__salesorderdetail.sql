with 

    source as (

        select * from {{ source('sources_aw', 'sales_salesorderdetail') }}

    ),

    renamed as (

        select
            cast(salesorderid as string) as sales_order_id
            , cast(salesorderdetailid as string) as sales_order_detail_id
            , {{ dbt_utils.generate_surrogate_key(['sales_order_detail_id']) }} as sales_sk
            , cast(carriertrackingnumber as string) as carrier_tracking_number
            , cast(orderqty as int) as order_quantity
            , cast(productid as string) as product_id
            , {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_fk
            , cast(specialofferid as string) as special_offer_id
            , cast(unitprice as float) as unit_price
            , cast(unitpricediscount as float) as unit_price_discount
            , cast(rowguid as string) as rowguid
            , try_cast(modifieddate as timestamp) as modified_date

        from source

    )

select * from renamed