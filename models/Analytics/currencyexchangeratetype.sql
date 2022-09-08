{{ config(alias='CURRENCYEXCHANGERATETYPE',
          tags='analytics_others'
         ) }}

SELECT 
c.clientid
, cert.CURRENCYEXCHANGERATETYPEID
, cert.TIMEPERIODID
, cert.LABEL
, cert.CODE
,current_timestamp ROW_INSERT_TS
FROM POC.RAW.CURRENCYEXCHANGERATETYPE cert
join poc.analytics.clients c
  on cert.client_nm =c.name
