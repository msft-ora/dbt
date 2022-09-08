{{ config(alias='ITEMTYPE',
          tags='analytics_items'
         ) }}

select distinct 
  c.clientid
  ,it.itemtype
,current_timestamp ROW_INSERT_TS
from POC.RAW.ITEMSTYPES it
join poc.analytics.clients c
  on it.client_nm =c.name
