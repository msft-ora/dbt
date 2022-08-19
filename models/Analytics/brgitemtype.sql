{{
  config(materialized='table' 
)
}}

select  
c.clientid
,it.itemid
,it.itemtype
,current_timestamp
from poc.raw.itemstypes it
join poc.analytics.clients c
on it.client_nm =c.name