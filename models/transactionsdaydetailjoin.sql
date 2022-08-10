{{ config(materialized='table') }}



WITH

transactionsdaydetail_schema AS

(SELECT SITEID

FCID

    FROM HVMG.transactionsdaydetail

    union

    select SITEID

FCID from LANDING.transactionsdaydetail)



select * from transactionsdaydetail_schema