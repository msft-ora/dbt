{{
  config(materialized='table'   ,
  schema='ANALYTICS'
)
}}

SELECT 
  c.clientid
  , t.SiteID
  , t.FCID
  , t.Date
  , t.TimeChange
  , t.TimeChangeEnd
  , t.ItemID
  , t.Qty
  , t.Amt
  , t.ChangedBy
  , t.Factor
,current_timestamp ROW_INSERT_TS
FROM poc.hvmg_raw."FC.TRANSACTIONSDAYDETAIL" t
join poc.analytics.clients c
  on t.client_nm =c.name

  union all
 select c.clientid,s.siteID,-3,t.date,getdate(),null,i.itemid,t.qty,t.Amt,0,0,current_timestamp ROW_INSERT_TS from 
poc.hvmg_raw.TRANSACTIONS t
join poc.analytics.clients c
  on t.client_nm =c.name
join poc.hvmg_raw.sites s
on t.siteid = s.siteTag     and t.client_nm=s.client_nm
join poc.hvmg_raw.items i
on  t.Tag = i.itemTag     and t.client_nm=i.client_nm