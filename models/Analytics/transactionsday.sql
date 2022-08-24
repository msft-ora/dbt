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
FROM poc.hvmg_raw.TransactionsDay t
join poc.analytics.clients c
  on t.client_nm =c.name