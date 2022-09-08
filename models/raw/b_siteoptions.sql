{{ config(alias='SITEOPTIONS',
          tags=['siteoptions']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.SITEID,
       a.FCID,
       a.ACCESSLEVEL,
       a.OPTIONSCSV,
       a.STARTYEAR,
       a.STARTPERIOD,
       a.ENDYEAR,
       a.ENDPERIOD,
       a.UPDATEDAILYACTUALSSETTIME,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."SITEOPTIONS"  a
MINUS
SELECT r.CLIENT_NM,
       r.SITEID,
       r.FCID,
       r.ACCESSLEVEL,
       r.OPTIONSCSV,
       r.STARTYEAR,
       r.STARTPERIOD,
       r.ENDYEAR,
       r.ENDPERIOD,
       r.UPDATEDAILYACTUALSSETTIME,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."SITEOPTIONS"  r
  JOIN (SELECT SITEID,
               FCID,
               ACCESSLEVEL,
               OPTIONSCSV,
               STARTYEAR,
               STARTPERIOD,
               ENDYEAR,
               ENDPERIOD,
               UPDATEDAILYACTUALSSETTIME,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."SITEOPTIONS" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.SITEID = d.SITEID
    and r.FCID = d.FCID
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       SITEID,
       FCID,
       ACCESSLEVEL,
       OPTIONSCSV,
       STARTYEAR,
       STARTPERIOD,
       ENDYEAR,
       ENDPERIOD,
       UPDATEDAILYACTUALSSETTIME,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."SITEOPTIONS" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
