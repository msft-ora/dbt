{{
  config(materialized='table'  ,
  schema='ANALYTICS'
)
}}


SELECT 
  c.clientid
  , s.SITEID
  , s.ID
  , s.SEGMENTTYPE
  , s.SEGMENTVALUE
,current_timestamp ROW_INSERT_TS
FROM POC.HVMG_RAW.SEGMENTATIONS s
join poc.analytics.clients c 
  on s.client_nm =c.name