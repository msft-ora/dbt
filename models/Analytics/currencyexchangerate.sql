
{{
  config(materialized='table' 
)
}}

SELECT 
    c.clientid
    , ce.CurrencyExchangeRateId
    , ce.DefaultCurrencyId
    , ce.ReportingCurrencyId
    , ce.CurrencyExchangeRateTypeId
    , ce.IsActive
FROM poc.raw.CurrencyExchangeRate ce
join poc.analytics.clients c
  on ce.client_nm =c.name
