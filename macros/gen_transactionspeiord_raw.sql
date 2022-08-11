{% macro gen_transactionsperiod_raw(in_client_name, in_file_name) %}
    '{{in_client_name}}'  client_nm,
    $1:SiteID siteid,
    $1:FCID fcid,
    $1:Year year,
    $1:Period period,
    $1:TimeChange TimeChange,
    $1:TimeChangeEnd TimeChangeEnd,
    $1:ItemID ItemID,
    $1:Qty Qty,
    $1:Amt Amt,
    $1:Factor Factor,
    $1:Rate Rate,
    $1:ChangedBy ChangedBy,
    current_timestamp row_insert_ts
    from @azureblobdata/fact/{{in_file_name}} 
    (file_format=> POC.HVMG.JSON)

{% endmacro %}