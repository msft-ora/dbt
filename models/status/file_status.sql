-- depends_on: {{ ref('calendar') }}
-- depends_on: {{ ref('calendarday') }}
-- depends_on: {{ ref('calendarperiod') }}
-- depends_on: {{ ref('currency') }}
-- depends_on: {{ ref('currencyexchangerate') }}
-- depends_on: {{ ref('currencyexchangerateitem') }}
-- depends_on: {{ ref('currencyexchangeratetype') }}
-- depends_on: {{ ref('items') }}
-- depends_on: {{ ref('itemslists') }}
-- depends_on: {{ ref('itemssite') }}
-- depends_on: {{ ref('itemstypes') }}
-- depends_on: {{ ref('laboritems') }}
-- depends_on: {{ ref('laborperson') }}
-- depends_on: {{ ref('labortypes') }}
-- depends_on: {{ ref('master') }}
-- depends_on: {{ ref('menupermissionstemplates') }}
-- depends_on: {{ ref('options') }}
-- depends_on: {{ ref('security') }}
-- depends_on: {{ ref('segmentations') }}
-- depends_on: {{ ref('siteoptions') }}
-- depends_on: {{ ref('sites') }}
-- depends_on: {{ ref('sitesaddresses') }}
-- depends_on: {{ ref('sitesbilling') }}
-- depends_on: {{ ref('sitesgroup') }}
-- depends_on: {{ ref('siteslists') }}
-- depends_on: {{ ref('siteslistsitems') }}
-- depends_on: {{ ref('sitesrooms') }}
-- depends_on: {{ ref('sitestypes') }}
-- depends_on: {{ ref('templateroles') }}
-- depends_on: {{ ref('transactions') }}
-- depends_on: {{ ref('transactionsdaydetail') }}
-- depends_on: {{ ref('transactionsmonth') }}
-- depends_on: {{ ref('transactionsperiod') }}
-- depends_on: {{ ref('types') }}

{{ config(alias='FILE_STATUS', 
          schema='ETL',
          tags=['calendar',
                'calendarday',
                'calendarperiod',
                'currency',
                'currencyexchangerate',
                'currencyexchangerateitem',
                'currencyexchangeratetype',
                'items',
                'itemslists',
                'itemssite',
                'itemstypes',
                'laboritems',
                'laborperson',
                'labortypes',
                'master',
                'menupermissionstemplates',
                'options',
                'security',
                'segmentations',
                'siteoptions',
                'sites',
                'sitesaddresses',
                'sitesbilling',
                'sitesgroup',
                'siteslists',
                'siteslistsitems',
                'sitesrooms',
                'sitestypes',
                'templateroles',
                'transactions',
                'transactionsdaydetail',
                'transactionsmonth',
                'transactionsperiod',
                'types']
         ) }}

/***********************************************************
 *  This query will update the FILE_STATUS to indicate 
 *  that these file(s) have been processed
************************************************************/
(
    SELECT a.FILE_NAME,
       a.FILE_STATUS,
       a.CLIENT_NM,
       a.TABLE_NAME,
       a.FILE_PROCESSED_TS
  FROM "POC"."ETL"."FILE_STATUS"  a
MINUS
SELECT s.FILE_NAME,
       s.FILE_STATUS,
       s.CLIENT_NM,
       s.TABLE_NAME,
       s.FILE_PROCESSED_TS
  FROM "POC"."ETL"."FILE_STATUS"  s
  JOIN {{ref('file_control')}}  ctrl
    ON s.FILE_NAME = ctrl.FILE_NAME and
       s.CLIENT_NM = ctrl.CLIENT_NM and
       s.TABLE_NAME = ctrl.TABLE_NAME
 WHERE s.FILE_STATUS IS NULL
 )
UNION
SELECT *
  FROM {{ref('file_control')}}  ctrl
