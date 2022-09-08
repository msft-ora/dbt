{{ config(alias='SITESLISTS',
          tags=['siteslists']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.ID,
       a.SITEID,
       a.SORT,
       a.LABEL,
       a.CALENDARID,
       a.TYPE,
       a.CONSOLIDATESITEID,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."SITESLISTS"  a
MINUS
SELECT r.CLIENT_NM,
       r.ID,
       r.SITEID,
       r.SORT,
       r.LABEL,
       r.CALENDARID,
       r.TYPE,
       r.CONSOLIDATESITEID,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."SITESLISTS"  r
  JOIN (SELECT ID,
               SITEID,
               SORT,
               LABEL,
               CALENDARID,
               TYPE,
               CONSOLIDATESITEID,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."SITESLISTS" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.ID = d.ID
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       ID,
       SITEID,
       SORT,
       LABEL,
       CALENDARID,
       TYPE,
       CONSOLIDATESITEID,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."SITESLISTS" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
