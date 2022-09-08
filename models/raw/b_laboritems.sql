{{ config(alias='LABORITEMS',
          tags=['laboritems']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.PERSONID,
       a.DATE,
       a.TYPEID,
       a.LOCID,
       a.QTY,
       a.AMT,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."LABORITEMS"  a
MINUS
SELECT r.CLIENT_NM,
       r.PERSONID,
       r.DATE,
       r.TYPEID,
       r.LOCID,
       r.QTY,
       r.AMT,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."LABORITEMS"  r
  JOIN (SELECT PERSONID,
               DATE,
               TYPEID,
               LOCID,
               QTY,
               AMT,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."LABORITEMS" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.PERSONID = d.PERSONID
    and r.DATE = d.DATE
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       PERSONID,
       DATE,
       TYPEID,
       LOCID,
       QTY,
       AMT,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."LABORITEMS" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
