with stock_history as (

    select * from {{ ref('stg_finance__stock_history') }}
        where indicator in ('post-market_close', 'pre-market_open','all-day_high','all-day_low', 'nasdaq_volume') 

),

pivoted as (

    select 
        company_symbol, 
        asset_class, 
        stock_exchange_name, 
        stock_date,         
        {{ dbt_utils.pivot(
      column = 'indicator',
      values = dbt_utils.get_column_values(ref('stg_finance__stock_history'), 'indicator'),
      then_value = 'stock_value'
            ) }}
    
    from stock_history
    group by company_symbol, asset_class, stock_exchange_name, stock_date
)

select * from pivoted
