{{
  config(materialized='table'  ,
  schema='ANALYTICS'
)
}}

SELECT 
  c.clientid
  , so.SITEID
  , so.FCID
  , so.ACCESSLEVEL
  , so.OPTIONSCSV
  , so.STARTYEAR
  , so.STARTPERIOD
  , so.ENDYEAR
  , so.ENDPERIOD
  , so.UPDATEDAILYACTUALSSETTIME

FROM POC.HVMG_RAW."FC.SITEOPTIONS" so
join poc.analytics.clients c
on so.client_nm =c.name