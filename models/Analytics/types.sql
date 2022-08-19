{{
  config(materialized='table' 
)
}}

SELECT 
c.clientid
, t.ID
, t.PID
, t.TAG
, t."SORT"
, t."TYPE"
, t.LABEL
, t."DATA"
, t."ROLES"
, t.ITEMID
, t.LASTMODIFIEDDATE

FROM POC.raw.TYPES t
join poc.analytics.clients c
on t.client_nm =c.name
