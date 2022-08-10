{{ config(materialized='table') }}



WITH
items_schema AS
(SELECT
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
ITEMTYPE
FROM  LANDING.items
    union
    select
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
ITEMTYPE from HVMG.items)



select * from items_schema