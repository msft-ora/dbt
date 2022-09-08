{{ config(alias='CALENDARDAY',
          tags=['calendarday']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CALENDARID,
       a.DATE,
       a.FISCALYEAR,
       a.FISCALPERIOD,
       a.FISCALDAY,
       a.FISCALPERIODSTART,
       a.FISCALPERIODSTOP,
       a.CLIENT_NM,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."CALENDARDAY"  a
MINUS
SELECT r.CALENDARID,
       r.DATE,
       r.FISCALYEAR,
       r.FISCALPERIOD,
       r.FISCALDAY,
       r.FISCALPERIODSTART,
       r.FISCALPERIODSTOP,
       r.CLIENT_NM,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."CALENDARDAY"  r
  JOIN (SELECT CALENDARID,
               DATE,
               FISCALYEAR,
               FISCALPERIOD,
               FISCALDAY,
               FISCALPERIODSTART,
               FISCALPERIODSTOP,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."CALENDARDAY" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.CALENDARID = d.CALENDARID
    and r.DATE = d.DATE
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT CALENDARID,
       DATE,
       FISCALYEAR,
       FISCALPERIOD,
       FISCALDAY,
       FISCALPERIODSTART,
       FISCALPERIODSTOP,
       land.CLIENT_NM AS CLIENT_NM,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."CALENDARDAY" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
