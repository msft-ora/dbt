{{ config(alias='TRANSACTIONS',
          tags=['transactions'],
          cluster_by=['CLIENT_NM','SITEID','TAG','DATE']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.SITEID,
       a.TAG,
       a.DATE,
       a.QTY,
       a.AMT,
       a.ADJAMT,
       a.FCQTY,
       a.FCAMT,
       a.FCFACTOR,
       a.FCRATE,
       a.BUDQTY,
       a.BUDAMT,
       a.BUDFACTOR,
       a.BUDRATE,
       a.CMTQTY,
       a.CMTAMT,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."TRANSACTIONS"  a
MINUS
SELECT r.CLIENT_NM,
       r.SITEID,
       r.TAG,
       r.DATE,
       r.QTY,
       r.AMT,
       r.ADJAMT,
       r.FCQTY,
       r.FCAMT,
       r.FCFACTOR,
       r.FCRATE,
       r.BUDQTY,
       r.BUDAMT,
       r.BUDFACTOR,
       r.BUDRATE,
       r.CMTQTY,
       r.CMTAMT,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."TRANSACTIONS"  r
  JOIN (SELECT SITEID,
               TAG,
               DATE,
               QTY,
               AMT,
               ADJAMT,
               FCQTY,
               FCAMT,
               FCFACTOR,
               FCRATE,
               BUDQTY,
               BUDAMT,
               BUDFACTOR,
               BUDRATE,
               CMTQTY,
               CMTAMT,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."TRANSACTIONS" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.SITEID = d.SITEID
    and r.TAG = d.TAG
    and r.DATE = d.DATE
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       SITEID,
       TAG,
       DATE,
       QTY,
       AMT,
       ADJAMT,
       FCQTY,
       FCAMT,
       FCFACTOR,
       FCRATE,
       BUDQTY,
       BUDAMT,
       BUDFACTOR,
       BUDRATE,
       CMTQTY,
       CMTAMT,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."TRANSACTIONS" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
