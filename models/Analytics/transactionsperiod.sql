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
  , t.YEAR
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
FROM POC.hvmg_raw."FC.TRANSACTIONSPERIOD" t
join poc.analytics.clients c
  on t.client_nm =c.name
union all
select c.clientid,s.siteID,-3,year,month,getdate(),null,i.itemid,qty,Amt,0,0,1,current_timestamp ROW_INSERT_TS from 
poc.hvmg_raw.TRANSACTIONSMONTH t
join poc.analytics.clients c
  on t.client_nm =c.name
join poc.hvmg_raw.sites s
on t.siteid = s.siteTag     and t.client_nm=s.client_nm
join poc.hvmg_raw.items i
on  t.Tag = i.itemTag     and t.client_nm=i.client_nm