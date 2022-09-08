{{ config(alias='CURRENCYEXCHANGERATE',
          tags='analytics_others'
         ) }}

SELECT 
    c.clientid
    , ce.CurrencyExchangeRateId
    , ce.DefaultCurrencyId
    , ce.ReportingCurrencyId
    , ce.CurrencyExchangeRateTypeId
    , ce.IsActive
    ,current_timestamp ROW_INSERT_TS
FROM POC.RAW.CURRENCYEXCHANGERATE ce
join poc.analytics.clients c
  on ce.client_nm =c.name
