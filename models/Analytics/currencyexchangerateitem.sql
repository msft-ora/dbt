
{{
  config(materialized='table' 
)
}}


SELECT 
c.clientid
, cc.CURRENCYEXCHANGERATEITEMID
, cc.CURRENCYEXCHANGERATEID
, cc.CURRENCYRATE
, cc.DATE

FROM POC.raw.CURRENCYEXCHANGERATEITEM cc
join poc.analytics.clients c
  on cc.client_nm =c.name

