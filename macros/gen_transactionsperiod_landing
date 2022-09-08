{% macro gen_transactionsperiod_landing(in_client_name, in_file_name) %}
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
    '{{in_client_name}}'  client_nm,
    current_timestamp row_insert_ts,
    'I' sys_change_operation,
    METADATA$FILENAME JSON_FILENAME
    from @azureblobdata/fact/{{in_file_name}} 
    (file_format=> POC.HVMG.JSON)

{% endmacro %}