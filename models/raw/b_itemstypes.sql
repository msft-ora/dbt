{{ config(alias='ITEMSTYPES',
          tags=['itemstypes']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.ITEMID,
       a.ITEMTYPE,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."ITEMSTYPES"  a
MINUS
SELECT r.CLIENT_NM,
       r.ITEMID,
       r.ITEMTYPE,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."ITEMSTYPES"  r
  JOIN (SELECT ITEMID,
               ITEMTYPE,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."ITEMSTYPES" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.ITEMID = d.ITEMID
    and r.ITEMTYPE = d.ITEMTYPE
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       ITEMID,
       ITEMTYPE,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."ITEMSTYPES" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
