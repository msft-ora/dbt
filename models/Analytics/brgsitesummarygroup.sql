{{
  config(materialized='table' 
)
}}

select
c.clientid
,sli.siteid
,sl.siteid sitegroupid
,sli.listid sitelistid
from poc.raw.siteslists sl
join poc.analytics.clients c 
on sl.client_nm =c.name
join poc.raw.siteslistsitems sli
on sl.client_nm=sli.client_nm and sl.id=sli.listid