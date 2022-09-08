{{ config(alias='SITES',
          tags=['sites']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.SITEID,
       a.SITETAG,
       a.SITENAME,
       a.NAMESHORT,
       a.SITEADDRESS1,
       a.SITEADDRESS2,
       a.SITECITY,
       a.SITESTATE,
       a.SITEZIP,
       a.SITEPHONE,
       a.FAX,
       a.SITEROOMS,
       a.SITEREGION,
       a.SITEOWNER,
       a.SITETYPE,
       a.SITELABEL,
       a.STARTDATE,
       a.BUD0FLAG,
       a.BUD1FLAG,
       a.BUD2FLAG,
       a.BUD3FLAG,
       a.CURRENCYID,
       a.ACTIVE,
       a.ISARCHIVED,
       a.ISCONSOLIDATED,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."SITES"  a
MINUS
SELECT r.CLIENT_NM,
       r.SITEID,
       r.SITETAG,
       r.SITENAME,
       r.NAMESHORT,
       r.SITEADDRESS1,
       r.SITEADDRESS2,
       r.SITECITY,
       r.SITESTATE,
       r.SITEZIP,
       r.SITEPHONE,
       r.FAX,
       r.SITEROOMS,
       r.SITEREGION,
       r.SITEOWNER,
       r.SITETYPE,
       r.SITELABEL,
       r.STARTDATE,
       r.BUD0FLAG,
       r.BUD1FLAG,
       r.BUD2FLAG,
       r.BUD3FLAG,
       r.CURRENCYID,
       r.ACTIVE,
       r.ISARCHIVED,
       r.ISCONSOLIDATED,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."SITES"  r
  JOIN (SELECT SITEID,
               SITETAG,
               SITENAME,
               NAMESHORT,
               SITEADDRESS1,
               SITEADDRESS2,
               SITECITY,
               SITESTATE,
               SITEZIP,
               SITEPHONE,
               FAX,
               SITEROOMS,
               SITEREGION,
               SITEOWNER,
               SITETYPE,
               SITELABEL,
               STARTDATE,
               BUD0FLAG,
               BUD1FLAG,
               BUD2FLAG,
               BUD3FLAG,
               CURRENCYID,
               ACTIVE,
               ISARCHIVED,
               ISCONSOLIDATED,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."SITES" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.SITEID = d.SITEID
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       SITEID,
       SITETAG,
       SITENAME,
       NAMESHORT,
       SITEADDRESS1,
       SITEADDRESS2,
       SITECITY,
       SITESTATE,
       SITEZIP,
       SITEPHONE,
       FAX,
       SITEROOMS,
       SITEREGION,
       SITEOWNER,
       SITETYPE,
       SITELABEL,
       STARTDATE,
       BUD0FLAG,
       BUD1FLAG,
       BUD2FLAG,
       BUD3FLAG,
       CURRENCYID,
       ACTIVE,
       ISARCHIVED,
       ISCONSOLIDATED,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."SITES" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
