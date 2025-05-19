select * from {{ ref('stg_finance__fx_rates') }} 
 where exchange_date >= '2018-05-01'
