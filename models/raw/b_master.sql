{{ config(alias='MASTER',
          tags=['master']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.FCID,
       a.DESCRIPTION,
       a.MODE,
       a.ISEDITABLE,
       a.ISACTIVE,
       a.SORT,
       a.STARTYEAR,
       a.STARTPERIOD,
       a.ENDYEAR,
       a.ENDPERIOD,
       a.OPTIONSCSV,
       a.ISWRITELOCKED,
       a.ROLES,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."MASTER"  a
MINUS
SELECT r.CLIENT_NM,
       r.FCID,
       r.DESCRIPTION,
       r.MODE,
       r.ISEDITABLE,
       r.ISACTIVE,
       r.SORT,
       r.STARTYEAR,
       r.STARTPERIOD,
       r.ENDYEAR,
       r.ENDPERIOD,
       r.OPTIONSCSV,
       r.ISWRITELOCKED,
       r.ROLES,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."MASTER"  r
  JOIN (SELECT FCID,
               DESCRIPTION,
               MODE,
               ISEDITABLE,
               ISACTIVE,
               SORT,
               STARTYEAR,
               STARTPERIOD,
               ENDYEAR,
               ENDPERIOD,
               OPTIONSCSV,
               ISWRITELOCKED,
               ROLES,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."MASTER" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.FCID = d.FCID
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       FCID,
       DESCRIPTION,
       MODE,
       ISEDITABLE,
       ISACTIVE,
       SORT,
       STARTYEAR,
       STARTPERIOD,
       ENDYEAR,
       ENDPERIOD,
       OPTIONSCSV,
       ISWRITELOCKED,
       ROLES,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."MASTER" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
