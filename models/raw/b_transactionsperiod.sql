{{ config(alias='TRANSACTIONSPERIOD',
          tags=['transactionsperiod'],
          cluster_by=['CLIENT_NM','SITEID','FCID','YEAR','PERIOD']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.SITEID,
       a.FCID,
       a.YEAR,
       a.PERIOD,
       a.TIMECHANGE,
       a.TIMECHANGEEND,
       a.ITEMID,
       a.QTY,
       a.AMT,
       a.FACTOR,
       a.RATE,
       a.CHANGEDBY,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."TRANSACTIONSPERIOD"  a
MINUS
SELECT r.CLIENT_NM,
       r.SITEID,
       r.FCID,
       r.YEAR,
       r.PERIOD,
       r.TIMECHANGE,
       r.TIMECHANGEEND,
       r.ITEMID,
       r.QTY,
       r.AMT,
       r.FACTOR,
       r.RATE,
       r.CHANGEDBY,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."TRANSACTIONSPERIOD"  r
  JOIN (SELECT land.CLIENT_NM AS CLIENT_NM,
               SITEID,
               FCID,
               YEAR,
               PERIOD,
               TIMECHANGE,
               TIMECHANGEEND,
               ITEMID,
               QTY,
               AMT,
               FACTOR,
               RATE,
               CHANGEDBY,
               ROW_INSERT_TS
          FROM "POC"."LANDING"."TRANSACTIONSPERIOD" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.SITEID = d.SITEID
    and r.FCID = d.FCID
    and r.YEAR = d.YEAR
    and r.PERIOD = d.PERIOD
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
       YEAR,
       PERIOD,
       TIMECHANGE,
       TIMECHANGEEND,
       ITEMID,
       QTY,
       AMT,
       FACTOR,
       RATE,
       CHANGEDBY,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."TRANSACTIONSPERIOD" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
