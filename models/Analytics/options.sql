{{ config(alias='OPTIONS',
          tags='analytics_others'
         ) }}

SELECT 
    c.clientid
    , o.ID
    , o.SITEID
    , o.NAME
    , o.VALUE
    ,current_timestamp ROW_INSERT_TS
FROM POC.RAW.OPTIONS o
join poc.analytics.clients c
on o.client_nm =c.name
