{{
  config(materialized='table'  ,
  schema='ANALYTICS',
  tags='item'
)
}}

SELECT 
    c.clientid
    ,i.itemid
    ,s.siteid
    , si.SITETAG
    , si.ITEMTAG
    , si.ITEMNAME
    , si.GLACCT
    , si.RATE
    , si.TYPE
    , si.TAG
    , si.ACTIVE
    , si.TAGUNIT
    , si.TAGMODE
    , si.TAGTYPE
    , si.ISBUDGETABLE
    , si.ISFORECASTABLE
    , si.ALTTAG1
    , si.ALTTAG2
    , si.STACCT
    , si.LASTMODIFIEDDATE
    , si.LASTUSERID
    , si.SCHEDULEID
    , current_timestamp ROW_INSERT_TS
FROM POC.LANDING.ITEMSSITE si
join poc.analytics.clients c 
    on si.client_nm =c.name
join {{ ref('sites') }} s
    on si.sitetag= s.sitetag
join {{ ref('items') }} i
    on si.itemtag=i.itemtag