{{
  config(materialized='table' ,
  schema='ANALYTICS',
  tags='site')
}}

SELECT 
  c.CLIENTID
  , sg.ID SITEGROUPID
  , sg.TAG SITEGROUPCODE
  , sg.NAME SITEGROUPNAME
,current_timestamp ROW_INSERT_TS
FROM POC.HVMG_RAW.SITESGROUP sg
join poc.analytics.clients c
  on sg.client_nm =c.name