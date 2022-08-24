{{
  config(materialized='table'  ,
  schema='ANALYTICS'
)
}}

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
FROM POC.hvmg_raw.TRANSACTIONSPERIOD t
join poc.analytics.clients c
  on t.client_nm =c.name
