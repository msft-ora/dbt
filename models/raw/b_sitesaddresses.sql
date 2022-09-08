{{ config(alias='SITESADDRESSES',
          tags=['sitesaddresses']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.ID,
       a.SITEID,
       a.TAG,
       a.NAME,
       a.ADDRESS,
       a.CITY,
       a.STATE,
       a.ZIP,
       a.COUNTRY,
       a.PHONE,
       a.FAX,
       a.LATITUDE,
       a.LONGITUDE,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."SITESADDRESSES"  a
MINUS
SELECT r.CLIENT_NM,
       r.ID,
       r.SITEID,
       r.TAG,
       r.NAME,
       r.ADDRESS,
       r.CITY,
       r.STATE,
       r.ZIP,
       r.COUNTRY,
       r.PHONE,
       r.FAX,
       r.LATITUDE,
       r.LONGITUDE,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."SITESADDRESSES"  r
  JOIN (SELECT ID,
               SITEID,
               TAG,
               NAME,
               ADDRESS,
               CITY,
               STATE,
               ZIP,
               COUNTRY,
               PHONE,
               FAX,
               LATITUDE,
               LONGITUDE,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."SITESADDRESSES" land
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
       TAG,
       NAME,
       ADDRESS,
       CITY,
       STATE,
       ZIP,
       COUNTRY,
       PHONE,
       FAX,
       LATITUDE,
       LONGITUDE,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."SITESADDRESSES" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
