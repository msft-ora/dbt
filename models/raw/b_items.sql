{{ config(alias='ITEMS',
          tags=['items']
         ) }}

/****************************************************************
 *  This query returns the target rows that we are KEEPING...
 *  (ie. ALL target rows MINUS the rows to be deleted)
*****************************************************************/
(
SELECT a.CLIENT_NM,
       a.ITEMID,
       a.PID,
       a.TYPEID,
       a.PORID1,
       a.PORID2,
       a.ITEMSORT,
       a.ITEMTAG,
       a.ITEMCLASS,
       a.CLASS,
       a.SUBCLASS,
       a.ITEMTYPE,
       a.ITEMNAME,
       a.ITEMGLACCT,
       a.BUDFLAG,
       a.BUDTAG,
       a.BUDFORMULA,
       a.STACCT,
       a.ALTTAG1,
       a.ALTTAG2,
       a.NAMESHORT,
       a.ROW_INSERT_TS
  FROM "POC"."RAW"."ITEMS"  a
MINUS
SELECT r.CLIENT_NM,
       r.ITEMID,
       r.PID,
       r.TYPEID,
       r.PORID1,
       r.PORID2,
       r.ITEMSORT,
       r.ITEMTAG,
       r.ITEMCLASS,
       r.CLASS,
       r.SUBCLASS,
       r.ITEMTYPE,
       r.ITEMNAME,
       r.ITEMGLACCT,
       r.BUDFLAG,
       r.BUDTAG,
       r.BUDFORMULA,
       r.STACCT,
       r.ALTTAG1,
       r.ALTTAG2,
       r.NAMESHORT,
       r.ROW_INSERT_TS
  FROM "POC"."RAW"."ITEMS"  r
  JOIN (SELECT ITEMID,
               PID,
               TYPEID,
               PORID1,
               PORID2,
               ITEMSORT,
               ITEMTAG,
               ITEMCLASS,
               CLASS,
               SUBCLASS,
               ITEMTYPE,
               ITEMNAME,
               ITEMGLACCT,
               BUDFLAG,
               BUDTAG,
               BUDFORMULA,
               STACCT,
               ALTTAG1,
               ALTTAG2,
               NAMESHORT,
               land.CLIENT_NM AS CLIENT_NM,
               ROW_INSERT_TS,
               SYS_CHANGE_OPERATION,
               JSON_FILENAME
          FROM "POC"."LANDING"."ITEMS" land
          JOIN {{ref('file_control')}}  ctrl
            ON land.JSON_FILENAME = ctrl.FILE_NAME and
               land.CLIENT_NM = ctrl.CLIENT_NM
         WHERE SYS_CHANGE_OPERATION = 'U')  d 
    ON r.CLIENT_NM = d.CLIENT_NM
    and r.ITEMID = d.ITEMID
)
UNION

/********************************************************************************
 *  This query grabs the NEW source rows that will be APPENDED to the target
 ********************************************************************************/
SELECT land.CLIENT_NM AS CLIENT_NM,
       ITEMID,
       PID,
       TYPEID,
       PORID1,
       PORID2,
       ITEMSORT,
       ITEMTAG,
       ITEMCLASS,
       CLASS,
       SUBCLASS,
       ITEMTYPE,
       ITEMNAME,
       ITEMGLACCT,
       BUDFLAG,
       BUDTAG,
       BUDFORMULA,
       STACCT,
       ALTTAG1,
       ALTTAG2,
       NAMESHORT,
       ctrl.FILE_PROCESSED_TS AS ROW_INSERT_TS
  FROM "POC"."LANDING"."ITEMS" land
  JOIN {{ref('file_control')}}  ctrl
    ON land.JSON_FILENAME = ctrl.FILE_NAME and
       land.CLIENT_NM = ctrl.CLIENT_NM
 WHERE SYS_CHANGE_OPERATION = 'I'
