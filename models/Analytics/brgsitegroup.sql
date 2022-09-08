{{ config(alias='BRGSITEGROUP',
          tags='analytics_sites'
         ) }}


select DISTINCT 
  s.clientid
  ,value sitegroupid
  ,siteid
  ,current_timestamp ROW_INSERT_TS
from {{ ref('sites') }} s, lateral split_to_table(sitetype, ' ')
WHERE sitegroupid<>''
