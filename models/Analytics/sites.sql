{{
  config(materialized='table'  ,
  schema='ANALYTICS',
  tags='site'
)
}}

SELECT 
  c.CLIENTID
  , s.SITEID
  , s.SITETAG
  , s.SITENAME
  --, s.NAMESHORT
  , sa.ADDRESS ADDRESS1
  , s.SITEADDRESS2
  , sa.CITY SITECITY
  , sa.STATE SITESTATE
  , sa.COUNTRY
  , sa.ZIP 
  , sa.LATITUDE
  , sa.LONGITUDE
  , sa.PHONE PHONENUMBER
  , sa.FAX FAXNUMBER
  , s.siterooms
  , s.siteregion region
  , s.siteowner owner
  , s.sitetype
  , s.active
  , sb.ENTITYTYPE
  , sb.startdate billingstartdate
  , sb.termed
  , sb.termedstart termedstartdate
  , sb.termedend termedenddate
  , sb.billable 
  , sb.billingtype
  , sb.exception
  , sb.exceptionreason
  , sb.units
,current_timestamp ROW_INSERT_TS
FROM POC.HVMG_RAW.SITES s
join poc.analytics.clients c
  on s.client_nm =c.name
left join POC.HVMG_RAW.SITESADDRESSES sa
  on s.client_nm=sa.client_nm and s.siteid=sa.siteid
left join POC.HVMG_RAW.SITESBILLING sb
  on s.client_nm=sb.client_nm and s.siteid=sb.siteid