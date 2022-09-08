{{ config(alias='ITEMSLISTS',
          tags=['itemslists']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.ID,
       a.PID,
       a.NEXTID,
       a.ITEMID,
       a.SORT,
       a.VALTYPE,
       a.STYLE,
       a.TYPE,
       a.LABEL,
       a.ROLES,
       a.PORTAG,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."ITEMSLISTS"  a
MINUS
SELECT r.CLIENT_NM,
       r.ID,
       r.PID,
       r.NEXTID,
       r.ITEMID,
       r.SORT,
       r.VALTYPE,
       r.STYLE,
       r.TYPE,
       r.LABEL,
       r.ROLES,
       r.PORTAG,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."ITEMSLISTS"  r
  JOIN (SELECT ID,
               PID,
               NEXTID,
               ITEMID,
               SORT,
               VALTYPE,
               STYLE,
               TYPE,
               LABEL,
               ROLES,
               PORTAG,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."ITEMSLISTS" land
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
       NEXTID,
       ITEMID,
       SORT,
       VALTYPE,
       STYLE,
       TYPE,
       LABEL,
       ROLES,
       PORTAG,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."ITEMSLISTS" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
