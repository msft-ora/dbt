{{ config(alias='MENUPERMISSIONSTEMPLATES',
          tags=['menupermissionstemplates']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.MENUPERMISSIONSTEMPLATEID,
       a.MENUPERMISSIONSTEMPLATELABEL,
       a.NAVMENUIDLIST,
       a.SORT,
       a.ACTIVE,
       a.ISCORP,
       a.ISSITE,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."MENUPERMISSIONSTEMPLATES"  a
MINUS
SELECT r.CLIENT_NM,
       r.MENUPERMISSIONSTEMPLATEID,
       r.MENUPERMISSIONSTEMPLATELABEL,
       r.NAVMENUIDLIST,
       r.SORT,
       r.ACTIVE,
       r.ISCORP,
       r.ISSITE,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."MENUPERMISSIONSTEMPLATES"  r
  JOIN (SELECT MENUPERMISSIONSTEMPLATEID,
               MENUPERMISSIONSTEMPLATELABEL,
               NAVMENUIDLIST,
               SORT,
               ACTIVE,
               ISCORP,
               ISSITE,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."MENUPERMISSIONSTEMPLATES" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.MENUPERMISSIONSTEMPLATEID = d.MENUPERMISSIONSTEMPLATEID
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       MENUPERMISSIONSTEMPLATEID,
       MENUPERMISSIONSTEMPLATELABEL,
       NAVMENUIDLIST,
       SORT,
       ACTIVE,
       ISCORP,
       ISSITE,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."MENUPERMISSIONSTEMPLATES" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
