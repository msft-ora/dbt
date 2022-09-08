{{ config(alias='BRGITEMTYPE',
          tags='analytics_items'
         ) }}

select  
  c.clientid
  ,it.itemid
  ,it.itemtype
  ,current_timestamp ROW_INSERT_TS
from poc.raw.itemstypes it
join poc.analytics.clients c
  on it.client_nm =c.name
