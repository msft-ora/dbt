{{ config(alias='CURRENCYEXCHANGERATEITEM',
          tags=['currencyexchangerateitem']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CURRENCYEXCHANGERATEITEMID,
       a.CURRENCYEXCHANGERATEID,
       a.CURRENCYRATE,
       a.DATE,
       a.CLIENT_NM,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."CURRENCYEXCHANGERATEITEM"  a
MINUS
SELECT r.CURRENCYEXCHANGERATEITEMID,
       r.CURRENCYEXCHANGERATEID,
       r.CURRENCYRATE,
       r.DATE,
       r.CLIENT_NM,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."CURRENCYEXCHANGERATEITEM"  r
  JOIN (SELECT CURRENCYEXCHANGERATEITEMID,
               CURRENCYEXCHANGERATEID,
               CURRENCYRATE,
               DATE,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."CURRENCYEXCHANGERATEITEM" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.CURRENCYEXCHANGERATEITEMID = d.CURRENCYEXCHANGERATEITEMID
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT CURRENCYEXCHANGERATEITEMID,
       CURRENCYEXCHANGERATEID,
       CURRENCYRATE,
       DATE,
       land.CLIENT_NM AS CLIENT_NM,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."CURRENCYEXCHANGERATEITEM" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
