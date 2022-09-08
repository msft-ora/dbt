{{ config(alias='TYPES',
          tags='analytics_others'
         ) }}

SELECT 
c.clientid
, t.ID
, t.PID
, t.TAG
, t.sort
, t.type
, t.LABEL
, t.DATA
, t.ROLES
, t.ITEMID
, t.LASTMODIFIEDDATE
,current_timestamp ROW_INSERT_TS
FROM POC.RAW.TYPES t
join poc.analytics.clients c
on t.client_nm =c.name
