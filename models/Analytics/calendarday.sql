
{{
  config(materialized='table'   ,
  schema='ANALYTICS'
)
}}


SELECT 
  c.clientid
  , cd.CALENDARID CALENDARTYPEID
  , cd.DATE
  , cd.FISCALYEAR
  , cd.FISCALPERIOD
  , cd.FISCALDAY
  , cd.FISCALPERIODSTART
  , cd.FISCALPERIODSTOP
  ,current_timestamp ROW_INSERT_TS
FROM POC.hvmg_raw.CALENDARDAY cd
 join poc.analytics.clients c
on cd.client_nm =c.name