with source as (

    select * from {{source('finance_economics','stock_price_timeseries')}}
), 

renamed as (

    select 

        TICKER as company_symbol,
        ASSET_CLASS as asset_class,
        PRIMARY_EXCHANGE_CODE as stock_exchange_code,
        PRIMARY_EXCHANGE_NAME as stock_exchange_name,
        VARIABLE as indicator,
        VARIABLE_NAME as indicator_name, 
        DATE as stock_date,
        VALUE as stock_value

    from source 

) 

select * from renamed
