
{{
  config(materialized='table'   ,
  schema='ANALYTICS'
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

FROM POC.hvmg_raw.CALENDAR cld
join poc.analytics.clients c
on cld.client_nm =c.name