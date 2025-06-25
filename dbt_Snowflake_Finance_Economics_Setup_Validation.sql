USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE transforming;

CREATE DATABASE analytics; -- where dbt materializes its SQL models

USE DATABASE FINANCE_ECONOMICS; -- accessed from Snowflake Marketplace
USE SCHEMA CYBERSYN;

-- SELECT * FROM STOCK_PRICE_TIMESERIES; -- 78.0m ROWS
SELECT * FROM STOCK_PRICE_TIMESERIES LIMIT 5;

SELECT * FROM STOCK_PRICE_TIMESERIES WHERE TICKER = 'AAPL' AND DATE = '2025-05-13';

SELECT DISTINCT VARIABLE FROM STOCK_PRICE_TIMESERIES;

SELECT MIN(Date) as start_date,
        MAX(Date) as end_date
from stock_price_timeseries;

-- YTD performance of a select group of stocks
WITH ytd_performance AS (
  SELECT
    ticker,
    MIN(date) OVER (PARTITION BY ticker) AS start_of_year_date,
    FIRST_VALUE(value) OVER (PARTITION BY ticker ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS start_of_year_price,
    MAX(date) OVER (PARTITION BY ticker) AS latest_date,
    LAST_VALUE(value) OVER (PARTITION BY ticker ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_price
  FROM cybersyn.stock_price_timeseries
  WHERE
    ticker IN ('AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA')
    AND date >= DATE_TRUNC('YEAR', CURRENT_DATE()) -- Truncates the current date to the start of the year
    AND variable_name = 'Post-Market Close'
)
SELECT
  ticker,
  start_of_year_date,
  start_of_year_price,
  latest_date,
  latest_price,
  (latest_price - start_of_year_price) / start_of_year_price * 100 AS percentage_change_ytd
FROM
  ytd_performance
GROUP BY
  ticker, start_of_year_date, start_of_year_price, latest_date, latest_price
ORDER BY percentage_change_ytd DESC;
;

-- dbt models

-- verify stg dbt model materialized as view
select * from analytics.information_schema.views
where table_name = 'STG_FINANCE__STOCK_HISTORY'; 


select * 
  from analytics.dbt_lcarabet.stg_finance__stock_history
 where company_symbol ='AAPL' 
   and stock_date ='2025-05-13';

-- verify int dbt model materialized as table
select * from analytics.information_schema.tables
where table_name = 'INT_FINANCE__STOCK_HISTORY';

select * 
  from analytics.dbt_lcarabet.int_finance__stock_history
 where company_symbol ='AAPL' 
   and stock_date ='2025-05-13';

SELECT MIN(Date) as start_date,
        MAX(Date) as end_date
from stock_price_timeseries;

select MIN(Date) as start_date,
        MAX(Date) as end_date
from fx_rates_timeseries;

select * from fx_rates_timeseries limit 5;

select * 
  from analytics.dbt_lcarabet.int_finance__stock_history_major_currency
 where company_symbol ='AAPL' 
   and stock_date ='2024-01-22';

select * from analytics.dbt_lcarabet.trading_book_gbp;

select * 
from analytics.dbt_lcarabet.int_finance__trading_daily_frequency_instrument_position_with_trades
where trader = 'Alex V.'
order by  book_date;

select * 
from analytics.dbt_lcarabet.fct_finance__trading_mv_pnl_incremental
where trader = 'Alex V.';

-- for tests
select company_symbol||stock_date||stock_exchange_name
from analytics.dbt_lcarabet.int_finance__stock_history
where company_symbol||stock_date||stock_exchange_name is not null;

select indicator, exchage_date, exchange_value
from analytics.dbt_lcarabet.stg_finance__fx_rates
where exchange_value < 0;

-- verify deployment
select * from analytics.information_schema.views
where table_schema = 'DBT_PRODUCTION' and table_name like 'STG_FINANCE__%';

select * from analytics.information_schema.tables
where table_schema = 'DBT_PRODUCTION' and table_name like 'TRADING_%';

select * from analytics.information_schema.tables
where table_schema = 'DBT_PRODUCTION' and table_name like 'INT_FINANCE__%';