{{ config(alias='TEMPLATEROLES',
          tags=['templateroles']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.TEMPID,
       a.ROLE,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."TEMPLATEROLES"  a
MINUS
SELECT r.CLIENT_NM,
       r.TEMPID,
       r.ROLE,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."TEMPLATEROLES"  r
  JOIN (SELECT TEMPID,
               ROLE,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."TEMPLATEROLES" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.TEMPID = d.TEMPID
    and r.ROLE = d.ROLE
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       TEMPID,
       ROLE,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."TEMPLATEROLES" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
