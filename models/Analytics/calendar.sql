{{ config(alias='CALENDAR',
          tags='analytics_others'
         ) }}

SELECT 
  c.clientid
  , cld.ID CALENDARTYPEID
  , cld.SORT
  , cld.LABEL
  , cld.STATUS
  , cld.WEEKSTART
  , cld.CLIENT_NM
  ,current_timestamp ROW_INSERT_TS
FROM POC.RAW.CALENDAR cld
join poc.analytics.clients c
on cld.client_nm =c.name
