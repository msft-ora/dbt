
{{
  config(materialized='table' 
)
}}

SELECT 
  c.clientid
  , cld.ID CALENDARTYPEID
  , cld.SORT
  , cld.LABEL
  , cld.STATUS
  , cld.WEEKSTART
  , cld.CLIENT_NM

FROM POC.raw.CALENDAR cld
join poc.analytics.clients c
on cld.client_nm =c.name