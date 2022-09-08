{{ config(alias='SITESGROUP',
          tags=['sitesgroup']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.ID,
       a.TAG,
       a.NAME,
       a.MENUID,
       a.PAGEID,
       a.TYPES,
       a.PID,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."SITESGROUP"  a
MINUS
SELECT r.CLIENT_NM,
       r.ID,
       r.TAG,
       r.NAME,
       r.MENUID,
       r.PAGEID,
       r.TYPES,
       r.PID,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."SITESGROUP"  r
  JOIN (SELECT ID,
               TAG,
               NAME,
               MENUID,
               PAGEID,
               TYPES,
               PID,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."SITESGROUP" land
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
       TAG,
       NAME,
       MENUID,
       PAGEID,
       TYPES,
       PID,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."SITESGROUP" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
