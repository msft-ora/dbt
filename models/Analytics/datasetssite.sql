{{
  config(materialized='table' 
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
,current_timestamp ROW_INSERT_TS
FROM POC.RAW."FC.SITEOPTIONS" so
join poc.analytics.clients c
on so.client_nm =c.name