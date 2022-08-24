
{{
  config(materialized='table'  ,
  schema='ANALYTICS'
)
}}

SELECT 
    c.clientid
    , ce.CurrencyExchangeRateId
    , ce.DefaultCurrencyId
    , ce.ReportingCurrencyId
    , ce.CurrencyExchangeRateTypeId
    , ce.IsActive
    ,current_timestamp ROW_INSERT_TS
FROM poc.hvmg_raw.CurrencyExchangeRate ce
join poc.analytics.clients c
  on ce.client_nm =c.name
