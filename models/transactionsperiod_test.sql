{{
  config(materialized='incremental')
}}


select
$1:SiteID,
$1:FCID,
$1:Year,
$1:Period,
$1:TimeChange,
$1:TimeChangeEnd,
$1:ItemID,
$1:Qty,
$1:Amt,
$1:Factor,
$1:Rate,
$1:ChangedBy,
'HVMG' client_nm,
'' row_insert_ts
from @azureblobdata/fact/HVMG_TRANSACTIONSPERIOD_2022-08-03_030739.json
(file_format=> POC.HVMG.JSON)
