{{ config(alias='TRANSACTIONSMONTH',
          tags=['transactionsmonth'],
          cluster_by=['CLIENT_NM','SITEID','YEAR','MONTH']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.SITEID,
       a.YEAR,
       a.MONTH,
       a.TAG,
       a.QTY,
       a.AMT,
       a.FCQTY,
       a.FCRATE,
       a.FCAMT,
       a.FCFACTOR,
       a.BUDQTY,
       a.BUDRATE,
       a.BUDAMT,
       a.BUDFACTOR,
       a.CMTQTY,
       a.CMTAMT,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."TRANSACTIONSMONTH"  a
MINUS
SELECT r.CLIENT_NM,
       r.SITEID,
       r.YEAR,
       r.MONTH,
       r.TAG,
       r.QTY,
       r.AMT,
       r.FCQTY,
       r.FCRATE,
       r.FCAMT,
       r.FCFACTOR,
       r.BUDQTY,
       r.BUDRATE,
       r.BUDAMT,
       r.BUDFACTOR,
       r.CMTQTY,
       r.CMTAMT,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."TRANSACTIONSMONTH"  r
  JOIN (SELECT SITEID,
               YEAR,
               MONTH,
               TAG,
               QTY,
               AMT,
               FCQTY,
               FCRATE,
               FCAMT,
               FCFACTOR,
               BUDQTY,
               BUDRATE,
               BUDAMT,
               BUDFACTOR,
               CMTQTY,
               CMTAMT,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."TRANSACTIONSMONTH" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.SITEID = d.SITEID
    and r.YEAR = d.YEAR
    and r.MONTH = d.MONTH
    and r.TAG = d.TAG
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       SITEID,
       YEAR,
       MONTH,
       TAG,
       QTY,
       AMT,
       FCQTY,
       FCRATE,
       FCAMT,
       FCFACTOR,
       BUDQTY,
       BUDRATE,
       BUDAMT,
       BUDFACTOR,
       CMTQTY,
       CMTAMT,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."TRANSACTIONSMONTH" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
