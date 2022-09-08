{% macro generate_file_control(in_table_name) %}
    FILE_NAME,
    'RAW' as FILE_STATUS,
    CLIENT_NM,
    TABLE_NAME,
    CURRENT_TIMESTAMP as FILE_PROCESSED_TS
    FROM "POC"."ETL"."FILE_STATUS"
    WHERE FILE_STATUS IS NULL
    AND TABLE_NAME = '{{in_table_name}}'

{% endmacro %}
