{{ config(alias='SITESSUMMARYGROUP',
          tags='analytics_sites'
         ) }}

select 
  c.clientid
  ,sl.siteid sitegroupid
  ,sl.id sitelistid
  ,sl.label
,current_timestamp ROW_INSERT_TS
from POC.RAW.SITESLISTS sl
join poc.analytics.clients c 
  on sl.client_nm =c.name
