{{
  config(materialized='table' 
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

FROM POC.raw.TRANSACTIONSPERIOD t
join poc.analytics.clients c
  on t.client_nm =c.name
