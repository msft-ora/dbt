{{ config(alias='ITEMS',
          tags='analytics_items'
         ) }}


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
,current_timestamp ROW_INSERT_TS
FROM POC.RAW.ITEMS i
join poc.analytics.clients c
  on i.client_nm =c.name
