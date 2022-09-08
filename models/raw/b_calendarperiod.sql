{{ config(alias='CALENDARPERIOD',
          tags=['calendarperiod']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CALENDARID,
       a.FISCALYEAR,
       a.FISCALPERIOD,
       a.PERIOD,
       a.SORT,
       a.FISCALPERIODSTART,
       a.FISCALPERIODSTOP,
       a.LABEL,
       a.LABELSHORT,
       a.LABELPERIOD,
       a.FISCALQUARTER,
       a.STANDARDCALENDARPERIODOFFSET,
       a.FISCALQUARTERSTART,
       a.FISCALQUARTERSTOP,
       a.FISCALTRIMESTER,
       a.FISCALBIMONTHLY,
       a.FISCALTRIMESTERSTART,
       a.FISCALTRIMESTERSTOP,
       a.FISCALBIMONTHLYSTART,
       a.FISCALBIMONTHLYSTOP,
       a.ACTUALYEAR,
       a.ACTUALMONTH,
       a.CLIENT_NM,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."CALENDARPERIOD"  a
MINUS
SELECT r.CALENDARID,
       r.FISCALYEAR,
       r.FISCALPERIOD,
       r.PERIOD,
       r.SORT,
       r.FISCALPERIODSTART,
       r.FISCALPERIODSTOP,
       r.LABEL,
       r.LABELSHORT,
       r.LABELPERIOD,
       r.FISCALQUARTER,
       r.STANDARDCALENDARPERIODOFFSET,
       r.FISCALQUARTERSTART,
       r.FISCALQUARTERSTOP,
       r.FISCALTRIMESTER,
       r.FISCALBIMONTHLY,
       r.FISCALTRIMESTERSTART,
       r.FISCALTRIMESTERSTOP,
       r.FISCALBIMONTHLYSTART,
       r.FISCALBIMONTHLYSTOP,
       r.ACTUALYEAR,
       r.ACTUALMONTH,
       r.CLIENT_NM,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."CALENDARPERIOD"  r
  JOIN (SELECT CALENDARID,
               FISCALYEAR,
               FISCALPERIOD,
               PERIOD,
               SORT,
               FISCALPERIODSTART,
               FISCALPERIODSTOP,
               LABEL,
               LABELSHORT,
               LABELPERIOD,
               FISCALQUARTER,
               STANDARDCALENDARPERIODOFFSET,
               FISCALQUARTERSTART,
               FISCALQUARTERSTOP,
               FISCALTRIMESTER,
               FISCALBIMONTHLY,
               FISCALTRIMESTERSTART,
               FISCALTRIMESTERSTOP,
               FISCALBIMONTHLYSTART,
               FISCALBIMONTHLYSTOP,
               ACTUALYEAR,
               ACTUALMONTH,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."CALENDARPERIOD" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.CALENDARID = d.CALENDARID
    and r.PERIOD = d.PERIOD
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT CALENDARID,
       FISCALYEAR,
       FISCALPERIOD,
       PERIOD,
       SORT,
       FISCALPERIODSTART,
       FISCALPERIODSTOP,
       LABEL,
       LABELSHORT,
       LABELPERIOD,
       FISCALQUARTER,
       STANDARDCALENDARPERIODOFFSET,
       FISCALQUARTERSTART,
       FISCALQUARTERSTOP,
       FISCALTRIMESTER,
       FISCALBIMONTHLY,
       FISCALTRIMESTERSTART,
       FISCALTRIMESTERSTOP,
       FISCALBIMONTHLYSTART,
       FISCALBIMONTHLYSTOP,
       ACTUALYEAR,
       ACTUALMONTH,
       land.CLIENT_NM AS CLIENT_NM,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."CALENDARPERIOD" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
