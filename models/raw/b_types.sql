{{ config(alias='TYPES',
          tags=['types']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.ID,
       a.PID,
       a.TAG,
       a.SORT,
       a.TYPE,
       a.LABEL,
       a.DATA,
       a.ROLES,
       a.ITEMID,
       a.LASTMODIFIEDDATE,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."TYPES"  a
MINUS
SELECT r.CLIENT_NM,
       r.ID,
       r.PID,
       r.TAG,
       r.SORT,
       r.TYPE,
       r.LABEL,
       r.DATA,
       r.ROLES,
       r.ITEMID,
       r.LASTMODIFIEDDATE,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."TYPES"  r
  JOIN (SELECT ID,
               PID,
               TAG,
               SORT,
               TYPE,
               LABEL,
               DATA,
               ROLES,
               ITEMID,
               LASTMODIFIEDDATE,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."TYPES" land
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
       PID,
       TAG,
       SORT,
       TYPE,
       LABEL,
       DATA,
       ROLES,
       ITEMID,
       LASTMODIFIEDDATE,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."TYPES" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
