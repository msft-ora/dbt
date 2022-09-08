{{ config(alias='LABORPERSON',
          tags=['laborperson']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.ID,
       a.SITEID,
       a.EMPID,
       a.NAME,
       a.TYPE,
       a.STATUS,
       a.BADGEID,
       a.ITEMID,
       a.ADDRESS1,
       a.ADDRESS2,
       a.CITY,
       a.STATE,
       a.ZIP,
       a.PHONE,
       a.EMAIL,
       a.SSN,
       a.PAYTYPE,
       a.MARITALSTATUS,
       a.DATEBIRTH,
       a.DATESTART,
       a.DATESTOP,
       a.DATEMODIFIED,
       a.NUMOFEXEMPTSPERSONAL,
       a.NUMOFEXEMPTSOTHER,
       a.ISREHIREABLE,
       a.STOPREASON,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."LABORPERSON"  a
MINUS
SELECT r.CLIENT_NM,
       r.ID,
       r.SITEID,
       r.EMPID,
       r.NAME,
       r.TYPE,
       r.STATUS,
       r.BADGEID,
       r.ITEMID,
       r.ADDRESS1,
       r.ADDRESS2,
       r.CITY,
       r.STATE,
       r.ZIP,
       r.PHONE,
       r.EMAIL,
       r.SSN,
       r.PAYTYPE,
       r.MARITALSTATUS,
       r.DATEBIRTH,
       r.DATESTART,
       r.DATESTOP,
       r.DATEMODIFIED,
       r.NUMOFEXEMPTSPERSONAL,
       r.NUMOFEXEMPTSOTHER,
       r.ISREHIREABLE,
       r.STOPREASON,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."LABORPERSON"  r
  JOIN (SELECT ID,
               SITEID,
               EMPID,
               NAME,
               TYPE,
               STATUS,
               BADGEID,
               ITEMID,
               ADDRESS1,
               ADDRESS2,
               CITY,
               STATE,
               ZIP,
               PHONE,
               EMAIL,
               SSN,
               PAYTYPE,
               MARITALSTATUS,
               DATEBIRTH,
               DATESTART,
               DATESTOP,
               DATEMODIFIED,
               NUMOFEXEMPTSPERSONAL,
               NUMOFEXEMPTSOTHER,
               ISREHIREABLE,
               STOPREASON,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."LABORPERSON" land
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
       EMPID,
       NAME,
       TYPE,
       STATUS,
       BADGEID,
       ITEMID,
       ADDRESS1,
       ADDRESS2,
       CITY,
       STATE,
       ZIP,
       PHONE,
       EMAIL,
       SSN,
       PAYTYPE,
       MARITALSTATUS,
       DATEBIRTH,
       DATESTART,
       DATESTOP,
       DATEMODIFIED,
       NUMOFEXEMPTSPERSONAL,
       NUMOFEXEMPTSOTHER,
       ISREHIREABLE,
       STOPREASON,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."LABORPERSON" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
