
{{
  config(materialized='table' 
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

FROM POC.raw.CALENDARDAY cd
 join poc.analytics.clients c
on cd.client_nm =c.name