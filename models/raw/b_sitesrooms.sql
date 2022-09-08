{{ config(alias='SITESROOMS',
          tags=['sitesrooms']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.SITEID,
       a.DATE,
       a.AMT,
       a.FCID,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."SITESROOMS"  a
MINUS
SELECT r.CLIENT_NM,
       r.SITEID,
       r.DATE,
       r.AMT,
       r.FCID,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."SITESROOMS"  r
  JOIN (SELECT SITEID,
               DATE,
               AMT,
               FCID,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."SITESROOMS" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.SITEID = d.SITEID
    and r.DATE = d.DATE
    and r.FCID = d.FCID
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       SITEID,
       DATE,
       AMT,
       FCID,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."SITESROOMS" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
