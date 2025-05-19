with source as (

    select * from {{source('finance_economics','fx_rates_timeseries')}}
),
 
renamed as (
 
select 
 
    BASE_CURRENCY_ID as base_currency_id,
    BASE_CURRENCY_NAME as base_currency_name,
    QUOTE_CURRENCY_ID as quote_currency_id,
    QUOTE_CURRENCY_NAME as quote_currency_name,
    BASE_CURRENCY_ID || '/' || QUOTE_CURRENCY_ID as indicator,
    VARIABLE_NAME AS indicator_name,
    DATE as exchange_date,
    VALUE as exchange_value

 
from source 
 
) 
 
select * from renamed
