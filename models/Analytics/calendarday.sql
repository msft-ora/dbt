{{ config(alias='CALENDARDAY',
          tags='analytics_others'
         ) }}

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
FROM POC.RAW.CALENDARDAY cd
 join poc.analytics.clients c
on cd.client_nm =c.name
