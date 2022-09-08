{{ config(alias='TRANSACTIONSDAYDETAIL',
          tags=['transactionsdaydetail'],
          cluster_by=['CLIENT_NM','SITEID','FCID','DATE']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.SITEID,
       a.FCID,
       a.DATE,
       a.TIMECHANGE,
       a.TIMECHANGEEND,
       a.ITEMID,
       a.QTY,
       a.AMT,
       a.CHANGEDBY,
       a.FACTOR,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."TRANSACTIONSDAYDETAIL"  a
MINUS
SELECT r.CLIENT_NM,
       r.SITEID,
       r.FCID,
       r.DATE,
       r.TIMECHANGE,
       r.TIMECHANGEEND,
       r.ITEMID,
       r.QTY,
       r.AMT,
       r.CHANGEDBY,
       r.FACTOR,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."TRANSACTIONSDAYDETAIL"  r
  JOIN (SELECT SITEID,
               FCID,
               DATE,
               TIMECHANGE,
               TIMECHANGEEND,
               ITEMID,
               QTY,
               AMT,
               CHANGEDBY,
               FACTOR,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."TRANSACTIONSDAYDETAIL" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.SITEID = d.SITEID
    and r.FCID = d.FCID
    and r.DATE = d.DATE
    and r.TIMECHANGE = d.TIMECHANGE
    and r.ITEMID = d.ITEMID
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       SITEID,
       FCID,
       DATE,
       TIMECHANGE,
       TIMECHANGEEND,
       ITEMID,
       QTY,
       AMT,
       CHANGEDBY,
       FACTOR,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."TRANSACTIONSDAYDETAIL" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
