{{ config(alias='CURRENCYEXCHANGERATE',
          tags=['currencyexchangerate']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CURRENCYEXCHANGERATEID,
       a.DEFAULTCURRENCYID,
       a.REPORTINGCURRENCYID,
       a.CURRENCYEXCHANGERATETYPEID,
       a.ISACTIVE,
       a.CLIENT_NM,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."CURRENCYEXCHANGERATE"  a
MINUS
SELECT r.CURRENCYEXCHANGERATEID,
       r.DEFAULTCURRENCYID,
       r.REPORTINGCURRENCYID,
       r.CURRENCYEXCHANGERATETYPEID,
       r.ISACTIVE,
       r.CLIENT_NM,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."CURRENCYEXCHANGERATE"  r
  JOIN (SELECT CURRENCYEXCHANGERATEID,
               DEFAULTCURRENCYID,
               REPORTINGCURRENCYID,
               CURRENCYEXCHANGERATETYPEID,
               ISACTIVE,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."CURRENCYEXCHANGERATE" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.CURRENCYEXCHANGERATEID = d.CURRENCYEXCHANGERATEID
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT CURRENCYEXCHANGERATEID,
       DEFAULTCURRENCYID,
       REPORTINGCURRENCYID,
       CURRENCYEXCHANGERATETYPEID,
       ISACTIVE,
       land.CLIENT_NM AS CLIENT_NM,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."CURRENCYEXCHANGERATE" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
