{{ config(alias='SITESBILLING',
          tags=['sitesbilling']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.ID,
       a.SITEID,
       a.ENTITYTYPE,
       a.STARTDATE,
       a.TERMED,
       a.TERMEDSTART,
       a.TERMEDEND,
       a.BILLABLE,
       a.BILLINGTYPE,
       a.EXCEPTION,
       a.EXCEPTIONREASON,
       a.UNITS,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."SITESBILLING"  a
MINUS
SELECT r.CLIENT_NM,
       r.ID,
       r.SITEID,
       r.ENTITYTYPE,
       r.STARTDATE,
       r.TERMED,
       r.TERMEDSTART,
       r.TERMEDEND,
       r.BILLABLE,
       r.BILLINGTYPE,
       r.EXCEPTION,
       r.EXCEPTIONREASON,
       r.UNITS,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."SITESBILLING"  r
  JOIN (SELECT ID,
               SITEID,
               ENTITYTYPE,
               STARTDATE,
               TERMED,
               TERMEDSTART,
               TERMEDEND,
               BILLABLE,
               BILLINGTYPE,
               EXCEPTION,
               EXCEPTIONREASON,
               UNITS,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."SITESBILLING" land
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
       SITEID,
       ENTITYTYPE,
       STARTDATE,
       TERMED,
       TERMEDSTART,
       TERMEDEND,
       BILLABLE,
       BILLINGTYPE,
       EXCEPTION,
       EXCEPTIONREASON,
       UNITS,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."SITESBILLING" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
