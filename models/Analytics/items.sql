{{
  config(materialized='table' 
)
}}


SELECT 
  c.CLIENTID
  , i.ITEMID
  , i.PID
  , i.TYPEID
  , i.PORID1
  , i.PORID2
  , i.ITEMSORT
  , i.ITEMTAG
  , i.ITEMCLASS
  , i.CLASS
  , i.SUBCLASS
  , i.ITEMTYPE
  , i.ITEMNAME
  , i.ITEMGLACCT
  , i.BUDFLAG
  , i.BUDTAG
  , i.BUDFORMULA
  , i.STACCT
  , i.ALTTAG1
  , i.ALTTAG2
  , i.NAMESHORT

FROM POC.RAW.ITEMS i
join poc.analytics.clients c
  on i.client_nm =c.name
