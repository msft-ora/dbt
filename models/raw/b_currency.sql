{{ config(alias='CURRENCY',
          tags=['currency']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.ID,
       a.CODE,
       a.LABEL,
       a.ISDEFAULTCURRENCY,
       a.ISREPORTINGCURRENCY,
       a.ISACTIVE,
       a.CLIENT_NM,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."CURRENCY"  a
MINUS
SELECT r.ID,
       r.CODE,
       r.LABEL,
       r.ISDEFAULTCURRENCY,
       r.ISREPORTINGCURRENCY,
       r.ISACTIVE,
       r.CLIENT_NM,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."CURRENCY"  r
  JOIN (SELECT ID,
               CODE,
               LABEL,
               ISDEFAULTCURRENCY,
               ISREPORTINGCURRENCY,
               ISACTIVE,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."CURRENCY" land
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
SELECT ID,
       CODE,
       LABEL,
       ISDEFAULTCURRENCY,
       ISREPORTINGCURRENCY,
       ISACTIVE,
       land.CLIENT_NM AS CLIENT_NM,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."CURRENCY" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
