{{
  config(materialized='table' 
)
}}


SELECT 
  c.clientid
  , s.SITEID
  , s.ID
  , s.SEGMENTTYPE
  , s.SEGMENTVALUE

FROM POC.RAW.SEGMENTATIONS s
join poc.analytics.clients c 
  on s.client_nm =c.name