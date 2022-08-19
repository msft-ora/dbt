{{
  config(materialized='table' 
)
}}

select 
c.clientid
,sl.siteid sitegroupid
,sl.id sitelistid
,sl.label
,sli.code
,sli.code2
,sli.code3
,sli.code4
,sli.code5
,sli.code6
,sli.isusecustom
,current_timestamp
from poc.raw.siteslists sl
join poc.analytics.clients c 
on sl.client_nm =c.name
join poc.raw.siteslistsitems sli
on sl.client_nm=sli.client_nm and sl.id=sli.listid