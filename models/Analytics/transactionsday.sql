{{
  config(materialized='table' 
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

FROM poc.raw.TransactionsDayDetail t
join poc.analytics.clients c
  on t.client_nm =c.name