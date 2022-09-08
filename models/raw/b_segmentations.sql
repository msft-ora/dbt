{{ config(alias='SEGMENTATIONS',
          tags=['segmentations']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.ID,
       a.SITEID,
       a.SEGMENTTYPE,
       a.SEGMENTVALUE,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."SEGMENTATIONS"  a
MINUS
SELECT r.CLIENT_NM,
       r.ID,
       r.SITEID,
       r.SEGMENTTYPE,
       r.SEGMENTVALUE,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."SEGMENTATIONS"  r
  JOIN (SELECT ID,
               SITEID,
               SEGMENTTYPE,
               SEGMENTVALUE,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."SEGMENTATIONS" land
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
       SEGMENTTYPE,
       SEGMENTVALUE,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."SEGMENTATIONS" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
