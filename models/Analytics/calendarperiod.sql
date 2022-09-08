{{ config(alias='CALENDARPERIOD',
          tags='analytics_others'
         ) }}

SELECT 
  c.clientid
  , cld.CALENDARID CALENDARTYPEID
  , cld.FISCALYEAR
  , cld.FISCALPERIOD
  , cld.PERIOD
  , cld.SORT
  , cld.FISCALPERIODSTART
  , cld.FISCALPERIODSTOP
  , cld.LABEL
  , cld.LABELSHORT
  , cld.LABELPERIOD
  , cld.FISCALQUARTER
  , cld.STANDARDCALENDARPERIODOFFSET
  , cld.FISCALQUARTERSTART
  , cld.FISCALQUARTERSTOP
  , cld.FISCALTRIMESTER
  , cld.FISCALBIMONTHLY
  , cld.FISCALTRIMESTERSTART
  , cld.FISCALTRIMESTERSTOP
  , cld.FISCALBIMONTHLYSTART
  , cld.FISCALBIMONTHLYSTOP
  , cld.ACTUALYEAR
  , cld.ACTUALMONTH
  ,current_timestamp ROW_INSERT_TS
FROM POC.RAW.CALENDARPERIOD cld
join poc.analytics.clients c
on cld.client_nm =c.name
