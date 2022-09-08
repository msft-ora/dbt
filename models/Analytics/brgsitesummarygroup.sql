{{ config(alias='BRGSITESUMMARYGROUP',
          tags='analytics_sites'
         ) }}

select
  c.clientid
  ,sli.siteid
  ,sl.siteid sitegroupid
  ,sli.listid sitelistid
  ,sli.code
  ,sli.code2
  ,sli.code3
  ,sli.code4
  ,sli.code5
  ,sli.code6
  ,sli.isusecustom
  ,current_timestamp ROW_INSERT_TS
from poc.raw.siteslists sl
join poc.analytics.clients c 
  on sl.client_nm =c.name
join poc.raw.siteslistsitems sli
  on sl.client_nm=sli.client_nm and sl.id=sli.listid
