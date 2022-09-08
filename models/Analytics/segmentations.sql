{{ config(alias='SEGMENTATIONS',
          tags='analytics_others'
         ) }}


SELECT 
  c.clientid
  , s.SITEID
  , s.ID
  , s.SEGMENTTYPE
  , s.SEGMENTVALUE
,current_timestamp ROW_INSERT_TS
FROM POC.RAW.SEGMENTATIONS s
join poc.analytics.clients c 
  on s.client_nm =c.name
