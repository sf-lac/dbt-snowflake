with
stock_history as (
    select * from {{ ref('int_finance__stock_history')}}
),
 
fx_rates as (
    select * from {{ ref('int_finance__fx_rates') }}
),
 
fx_rates_gdp as (
    select * from fx_rates
        where indicator = 'USD/GBP'   
),
 
fx_rates_eur as (
    select * from fx_rates
        where indicator = 'USD/EUR' 
),
 
joined as (
    select 
        stock_history.*,
        fx_rates_gdp.exchange_value * stock_history."pre-market_open" as gbp_open,       
        fx_rates_gdp.exchange_value * stock_history."all-day_high" as gbp_high,     
        fx_rates_gdp.exchange_value * stock_history."all-day_low" as gbp_low,   
        fx_rates_gdp.exchange_value * stock_history."post-market_close" as gbp_close,     
        fx_rates_eur.exchange_value * stock_history."pre-market_open" as eur_open,       
        fx_rates_eur.exchange_value * stock_history."all-day_high" as eur_high,     
        fx_rates_eur.exchange_value *stock_history."all-day_low" as eur_low,
        fx_rates_eur.exchange_value * stock_history."post-market_close" as eur_close    
    from stock_history
    left join fx_rates_gdp on stock_history.stock_date = fx_rates_gdp.exchange_date
    left join fx_rates_eur on stock_history.stock_date = fx_rates_eur.exchange_date
)

select * from joined