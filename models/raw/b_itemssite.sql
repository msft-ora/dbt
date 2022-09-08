{{ config(alias='ITEMSSITE',
          tags=['itemssite']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.ID,
       a.SITETAG,
       a.ITEMTAG,
       a.ITEMNAME,
       a.GLACCT,
       a.RATE,
       a.TYPE,
       a.TAG,
       a.ACTIVE,
       a.TAGUNIT,
       a.TAGMODE,
       a.TAGTYPE,
       a.ISBUDGETABLE,
       a.ISFORECASTABLE,
       a.ALTTAG1,
       a.ALTTAG2,
       a.STACCT,
       a.LASTMODIFIEDDATE,
       a.LASTUSERID,
       a.SCHEDULEID,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."ITEMSSITE"  a
MINUS
SELECT r.CLIENT_NM,
       r.ID,
       r.SITETAG,
       r.ITEMTAG,
       r.ITEMNAME,
       r.GLACCT,
       r.RATE,
       r.TYPE,
       r.TAG,
       r.ACTIVE,
       r.TAGUNIT,
       r.TAGMODE,
       r.TAGTYPE,
       r.ISBUDGETABLE,
       r.ISFORECASTABLE,
       r.ALTTAG1,
       r.ALTTAG2,
       r.STACCT,
       r.LASTMODIFIEDDATE,
       r.LASTUSERID,
       r.SCHEDULEID,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."ITEMSSITE"  r
  JOIN (SELECT ID,
               SITETAG,
               ITEMTAG,
               ITEMNAME,
               GLACCT,
               RATE,
               TYPE,
               TAG,
               ACTIVE,
               TAGUNIT,
               TAGMODE,
               TAGTYPE,
               ISBUDGETABLE,
               ISFORECASTABLE,
               ALTTAG1,
               ALTTAG2,
               STACCT,
               LASTMODIFIEDDATE,
               LASTUSERID,
               SCHEDULEID,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."ITEMSSITE" land
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
       SITETAG,
       ITEMTAG,
       ITEMNAME,
       GLACCT,
       RATE,
       TYPE,
       TAG,
       ACTIVE,
       TAGUNIT,
       TAGMODE,
       TAGTYPE,
       ISBUDGETABLE,
       ISFORECASTABLE,
       ALTTAG1,
       ALTTAG2,
       STACCT,
       LASTMODIFIEDDATE,
       LASTUSERID,
       SCHEDULEID,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."ITEMSSITE" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
