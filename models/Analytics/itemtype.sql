{{
  config(materialized='table' 
)
}}

select distinct 
  c.clientid
  ,it.itemtype

from poc.raw.itemstypes it
join poc.analytics.clients c
  on it.client_nm =c.name