{{
  config(materialized='table' 
)
}}


SELECT 
c.clientid
, cert.CURRENCYEXCHANGERATETYPEID
, cert.TIMEPERIODID
, cert.LABEL
, cert.CODE

FROM POC.raw.CURRENCYEXCHANGERATETYPE cert
join poc.analytics.clients c
  on cert.client_nm =c.name