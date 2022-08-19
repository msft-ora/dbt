{{
  config(materialized='table' 
)
}}

SELECT c.CLIENTID
, sg.ID SITEGROUPID
, sg.TAG SITEGROUPCODE
, sg.NAME SITEGROUPNAME
, current_timestamp
FROM POC.RAW.SITESGROUP sg
join poc.analytics.clients c
on sg.client_nm =c.name