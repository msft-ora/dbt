{{
  config(materialized='table'  ,
  schema='ANALYTICS',
  tags='site'
)
}}


select DISTINCT 
  s.clientid
  ,value sitegroupid
  ,siteid

from {{ ref('sites') }} s, lateral split_to_table(sitetype, ' ')
WHERE sitegroupid<>''