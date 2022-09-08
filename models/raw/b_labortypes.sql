{{ config(alias='LABORTYPES',
          tags=['labortypes']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.ID,
       a.SITEID,
       a.TYPE,
       a.CODE,
       a.LABEL,
       a.ITEMID,
       a.STANDARDTYPE,
       a.INTERFACEINDEX,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."LABORTYPES"  a
MINUS
SELECT r.CLIENT_NM,
       r.ID,
       r.SITEID,
       r.TYPE,
       r.CODE,
       r.LABEL,
       r.ITEMID,
       r.STANDARDTYPE,
       r.INTERFACEINDEX,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."LABORTYPES"  r
  JOIN (SELECT ID,
               SITEID,
               TYPE,
               CODE,
               LABEL,
               ITEMID,
               STANDARDTYPE,
               INTERFACEINDEX,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."LABORTYPES" land
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
       TYPE,
       CODE,
       LABEL,
       ITEMID,
       STANDARDTYPE,
       INTERFACEINDEX,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."LABORTYPES" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
