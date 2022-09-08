{{ config(alias='SITESLISTSITEMS',
          tags=['siteslistsitems']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.LISTID,
       a.SITEID,
       a.CODE,
       a.CODE2,
       a.CODE3,
       a.CODE4,
       a.CODE5,
       a.CODE6,
       a.ISUSECUSTOM,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."SITESLISTSITEMS"  a
MINUS
SELECT r.CLIENT_NM,
       r.LISTID,
       r.SITEID,
       r.CODE,
       r.CODE2,
       r.CODE3,
       r.CODE4,
       r.CODE5,
       r.CODE6,
       r.ISUSECUSTOM,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."SITESLISTSITEMS"  r
  JOIN (SELECT LISTID,
               SITEID,
               CODE,
               CODE2,
               CODE3,
               CODE4,
               CODE5,
               CODE6,
               ISUSECUSTOM,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."SITESLISTSITEMS" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.LISTID = d.LISTID
    and r.SITEID = d.SITEID
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       LISTID,
       SITEID,
       CODE,
       CODE2,
       CODE3,
       CODE4,
       CODE5,
       CODE6,
       ISUSECUSTOM,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."SITESLISTSITEMS" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
