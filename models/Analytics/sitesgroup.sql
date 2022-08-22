{{
  config(materialized='table' ,
  schema='ANALYTICS')
}}

SELECT 
  c.CLIENTID
  , sg.ID SITEGROUPID
  , sg.TAG SITEGROUPCODE
  , sg.NAME SITEGROUPNAME

FROM POC.HVMG_RAW.SITESGROUP sg
join poc.analytics.clients c
  on sg.client_nm =c.name