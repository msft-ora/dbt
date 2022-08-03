{{
  config(materialized='incremental')
}}


select
$1:SiteID siteid,
$1:FCID fcid,
$1:Year year,
$1:Period period,
$1:TimeChange TimeChange,
$1:TimeChangeEnd TimeChangeEnd,
$1:ItemID ItemID,
$1:Qty Qty,
$1:Amt Amt,
$1:Factor Factor,
$1:Rate Rate,
$1:ChangedBy ChangedBy,
'HVMG' client_nm,
current_timestamp row_insert_ts
from @azureblobdata/fact/HVMG_TRANSACTIONSPERIOD_2022-08-03_030739.json
(file_format=> POC.HVMG.JSON)
