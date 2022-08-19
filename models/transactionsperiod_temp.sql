{{
  config(materialized='view')
}}


select {{ gen_transactionsperiod_landing( 'HVMG', 'HVMG_TRANSACTIONSPERIOD_2022-08-18_210301.json' ) }}