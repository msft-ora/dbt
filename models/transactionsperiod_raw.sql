{{
  config(materialized='incremental'  ,
  pre_hook="delete from {{this}} t using {{ ref('transactionsperiod_temp') }} s 
  where t.client_nm=s.client_nm and t.siteid=s.siteid and t.fcid=s.fcid and t.year=s.year and t.period=s.period and t.itemid=s.itemid and t.TimeChangeEnd=s.TimeChange and t.TimeChangeEnd is null  "

)
}}

select {{ gen_transactionsperiod_raw( 'HVMG', 'HVMG_TRANSACTIONSPERIOD_2022-08-18_210301.json' ) }}