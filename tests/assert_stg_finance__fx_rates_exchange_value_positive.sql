-- exchange rates should be >=0
select 
    indicator,
    exchange_date,
    exchange_value
from {{ ref ('stg_finance__fx_rates') }}
where exchange_value < 0