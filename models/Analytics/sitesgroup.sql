{{ config(alias='SITESGROUP',
          tags='analytics_sites'
         ) }}

SELECT 
  c.CLIENTID
  , sg.ID SITEGROUPID
  , sg.TAG SITEGROUPCODE
  , sg.NAME SITEGROUPNAME
,current_timestamp ROW_INSERT_TS
FROM POC.RAW.SITESGROUP sg
join poc.analytics.clients c
  on sg.client_nm =c.name
