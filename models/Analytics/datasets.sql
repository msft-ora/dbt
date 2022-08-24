{{
  config(materialized='table'  ,
  schema='ANALYTICS'
)
}}

SELECT 
  c.clientid
  , m.FCID
  , m.DESCRIPTION
  , m.MODE
  , m.ISEDITABLE
  , m.ISACTIVE
  , m.SORT
  , m.STARTYEAR
  , m.STARTPERIOD
  , m.ENDYEAR
  , m.ENDPERIOD
  , m.OPTIONSCSV
  , m.ISWRITELOCKED
  , m.ROLES
,current_timestamp ROW_INSERT_TS
FROM POC.HVMG_RAW."FC.MASTER"  m
join poc.analytics.clients c
  on m.client_nm =c.name