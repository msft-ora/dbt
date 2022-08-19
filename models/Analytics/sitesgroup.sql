{{
  config(materialized='table' 
)
}}

SELECT c.CLIENTID
, sg.ID
, sg.TAG
, sg.NAME
, sg.MENUID
, sg.PAGEID
, sg.TYPES
, sg.PID
, current_timestamp
FROM POC.RAW.SITESGROUP sg
join poc.analytics.clients c
on sg.client_nm =c.name