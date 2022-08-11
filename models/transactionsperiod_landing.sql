{{
  config(materialized='incremental')
}}


select {{ gen_transactionsperiod_landing( 'HVMG', 'HVMG_TRANSACTIONSPERIOD_2022-08-03_030739.json' ) }}