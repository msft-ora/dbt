{{ config(alias='CURRENCYEXCHANGERATETYPE',
          tags=['currencyexchangeratetype']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.CURRENCYEXCHANGERATETYPEID,
       a.TIMEPERIODID,
       a.LABEL,
       a.CODE,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."CURRENCYEXCHANGERATETYPE"  a
MINUS
SELECT r.CLIENT_NM,
       r.CURRENCYEXCHANGERATETYPEID,
       r.TIMEPERIODID,
       r.LABEL,
       r.CODE,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."CURRENCYEXCHANGERATETYPE"  r
  JOIN (SELECT CURRENCYEXCHANGERATETYPEID,
               TIMEPERIODID,
               LABEL,
               CODE,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."CURRENCYEXCHANGERATETYPE" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.CURRENCYEXCHANGERATETYPEID = d.CURRENCYEXCHANGERATETYPEID
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       CURRENCYEXCHANGERATETYPEID,
       TIMEPERIODID,
       LABEL,
       CODE,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."CURRENCYEXCHANGERATETYPE" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
