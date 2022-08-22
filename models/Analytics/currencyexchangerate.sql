
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
FROM poc.hvmg_raw.CurrencyExchangeRate ce
join poc.analytics.clients c
  on ce.client_nm =c.name
