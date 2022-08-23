{{
  config(materialized='table'  ,
  schema='ANALYTICS',
  tags='item'
)
}}

select  
  c.clientid
  ,it.itemid
  ,it.itemtype

from poc.hvmg_raw.itemstypes it
join poc.analytics.clients c
  on it.client_nm =c.name