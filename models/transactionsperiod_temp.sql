{{
  config(materialized='view')
}}


select {{ generate_select_sql( 'HVMG', 'HVMG_TRANSACTIONSPERIOD_2022-08-03_030739.json' ) }}