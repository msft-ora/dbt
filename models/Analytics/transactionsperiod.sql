{{ config(alias='TRANSACTIONSPERIOD',
          tags='analytics_transactions',
          cluster_by=['CLIENTID','SITEID','FCID','YEAR','PERIOD']
         ) }}

SELECT 
  c.clientid
  --, t.CALENDARID CALENDARTYPEID
  , t.SITEID
  , t.FCID
  , t."YEAR"
  , t.PERIOD
  , t.TIMECHANGE
  , t.TIMECHANGEEND
  , t.ITEMID
  , t.QTY
  , t.AMT
  , t.FACTOR
  , t.RATE
  , t.CHANGEDBY
,current_timestamp ROW_INSERT_TS
FROM POC.RAW.TRANSACTIONSPERIOD t
join poc.analytics.clients c
  on t.client_nm =c.name
