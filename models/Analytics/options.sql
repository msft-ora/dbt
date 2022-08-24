{{
  config(materialized='table'  ,
  schema='ANALYTICS'
)
}}

SELECT 
    c.clientid
    , o.ID
    , o.SITEID
    , o.NAME
    , o.VALUE
    ,current_timestamp ROW_INSERT_TS
FROM POC.HVMG_raw.OPTIONS o
join poc.analytics.clients c
on o.client_nm =c.name