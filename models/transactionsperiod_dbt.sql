{{
  config(materialized='table'  
  )
}}


select
 client_nm,
 siteid,
 fcid,
year,
 period,
TimeChange,
 TimeChangeEnd,
 ItemID,
 Qty,
 Amt,
 Factor,
 Rate,
 ChangedBy,
current_timestamp row_insert_ts
from {{ ref('transactionsperiod_test') }}

