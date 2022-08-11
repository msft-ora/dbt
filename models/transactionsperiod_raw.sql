{{
  config(materialized='incremental'  ,
  pre_hook="delete from {{this}} t using {{ ref('transactionsperiod_temp') }} s 
  where t.client_nm=s.client_nm and t.siteid=s.siteid and t.fcid=s.fcid and t.year=s.year and t.period=s.period and t.TimeChangeEnd=s.TimeChange and t.TimeChangeEnd is null  "

)
}}

--     delete from {{this}} t using {{ ref('transactionsperiod_test') }} s 
--   where t.client_nm=s.client_nm and t.siteid=s.siteid and t.fcid=s.fcid and t.year=s.year and t.period=s.period and t.TimeChange=s.TimeChange and t.TimeChangeEnd =s.TimeChangeEnd

select {{ gen_transactionsperiod_raw( 'HVMG', 'HVMG_TRANSACTIONSPERIOD_2022-08-03_030739.json' ) }}