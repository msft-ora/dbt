
{{
  config(materialized='table'  ,
  schema='ANALYTICS'
)
}}

SELECT 
c.clientid
, cr.ID
, cr.CODE
, cr.LABEL
, cr.ISDEFAULTCURRENCY
, cr.ISREPORTINGCURRENCY
, cr.ISACTIVE
,current_timestamp ROW_INSERT_TS
FROM POC.hvmg_raw.CURRENCY cr
join poc.analytics.clients c
  on cr.client_nm =c.name