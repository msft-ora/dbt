{{ config(alias='CURRENCYEXCHANGERATEITEM',
          tags='analytics_others'
         ) }}

SELECT 
c.clientid
, cc.CURRENCYEXCHANGERATEITEMID
, cc.CURRENCYEXCHANGERATEID
, cc.CURRENCYRATE
, cc.DATE
,current_timestamp ROW_INSERT_TS
FROM POC.RAW.CURRENCYEXCHANGERATEITEM cc
join poc.analytics.clients c
  on cc.client_nm =c.name
