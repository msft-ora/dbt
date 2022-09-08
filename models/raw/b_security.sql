{{ config(alias='SECURITY',
          tags=['security']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.ID,
       a.USERNAME,
       a.FIRSTNAME,
       a.LASTNAME,
       a.SITEGROUPTAG,
       a.ROLES,
       a.EMAIL,
       a.DEPTS,
       a.USERPASSWORD,
       a.SALT,
       a.RESETPASSWORD,
       a.PASSWORDCHANGEDATE,
       a.STATUS,
       a.COMMENT,
       a.ISPSADMIN,
       a.ISPSDEV,
       a.MENUTEMPLATE,
       a.LOGINATTEMPTS,
       a.LOGINATTEMPTDATE,
       a.EMPLOYEEID,
       a.ISCOAADMIN,
       a.USERGUID,
       a.LASTLOGIN,
       a.PENDOGUID,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."SECURITY"  a
MINUS
SELECT r.CLIENT_NM,
       r.ID,
       r.USERNAME,
       r.FIRSTNAME,
       r.LASTNAME,
       r.SITEGROUPTAG,
       r.ROLES,
       r.EMAIL,
       r.DEPTS,
       r.USERPASSWORD,
       r.SALT,
       r.RESETPASSWORD,
       r.PASSWORDCHANGEDATE,
       r.STATUS,
       r.COMMENT,
       r.ISPSADMIN,
       r.ISPSDEV,
       r.MENUTEMPLATE,
       r.LOGINATTEMPTS,
       r.LOGINATTEMPTDATE,
       r.EMPLOYEEID,
       r.ISCOAADMIN,
       r.USERGUID,
       r.LASTLOGIN,
       r.PENDOGUID,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."SECURITY"  r
  JOIN (SELECT ID,
               USERNAME,
               FIRSTNAME,
               LASTNAME,
               SITEGROUPTAG,
               ROLES,
               EMAIL,
               DEPTS,
               USERPASSWORD,
               SALT,
               RESETPASSWORD,
               PASSWORDCHANGEDATE,
               STATUS,
               COMMENT,
               ISPSADMIN,
               ISPSDEV,
               MENUTEMPLATE,
               LOGINATTEMPTS,
               LOGINATTEMPTDATE,
               EMPLOYEEID,
               ISCOAADMIN,
               USERGUID,
               LASTLOGIN,
               PENDOGUID,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."SECURITY" land
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
       USERNAME,
       FIRSTNAME,
       LASTNAME,
       SITEGROUPTAG,
       ROLES,
       EMAIL,
       DEPTS,
       USERPASSWORD,
       SALT,
       RESETPASSWORD,
       PASSWORDCHANGEDATE,
       STATUS,
       COMMENT,
       ISPSADMIN,
       ISPSDEV,
       MENUTEMPLATE,
       LOGINATTEMPTS,
       LOGINATTEMPTDATE,
       EMPLOYEEID,
       ISCOAADMIN,
       USERGUID,
       LASTLOGIN,
       PENDOGUID,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."SECURITY" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
