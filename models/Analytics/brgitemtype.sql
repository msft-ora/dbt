{{
  config(materialized='table' 
)
}}

select  
  c.clientid
  ,it.itemid
  ,it.itemtype

from poc.raw.itemstypes it
join poc.analytics.clients c
  on it.client_nm =c.name