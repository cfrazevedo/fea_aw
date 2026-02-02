with 

    source as (

        select * from {{ source('sources_aw', 'sales_salesorderdetail') }}

    ),

    renamed as (

        select
            cast(salesorderid as string) as sales_order_id
            , cast(salesorderdetailid as string) as sales_order_detail_id
            , cast(carriertrackingnumber as string) as carrier_tracking_number
            , cast(orderqty as int) as order_quantity
            , cast(productid as string) as product_id
            , cast(specialofferid as string) as special_offer_id
            , cast(unitprice as float) as unit_price
            , cast(unitpricediscount as float) as unit_price_discount
            , cast(rowguid as string) as rowguid
            , try_cast(modifieddate as timestamp) as modified_date

        from source

    )

select * from renamed