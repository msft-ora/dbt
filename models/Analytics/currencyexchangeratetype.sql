{{
  config(materialized='table'  ,
  schema='ANALYTICS'
)
}}


SELECT 
c.clientid
, cert.CURRENCYEXCHANGERATETYPEID
, cert.TIMEPERIODID
, cert.LABEL
, cert.CODE

FROM POC.hvmg_raw.CURRENCYEXCHANGERATETYPE cert
join poc.analytics.clients c
  on cert.client_nm =c.name