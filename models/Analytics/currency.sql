
{{
  config(materialized='table' 
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

FROM POC.raw.CURRENCY cr
join poc.analytics.clients c
  on cr.client_nm =c.name