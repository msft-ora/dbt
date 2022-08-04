{% macro external_stage(path='@azureblobdata/fact/') %}
    {{path}}
{% endmacro %}

-- {% macro ensure_external_stage(stage_name, s3_url, file_format, temporary=False) %}
--     {{ log('Making external stage: ' ~ [stage_name, s3_url, file_format, temporary] | join(', ')) }}
--     create or replace stage {{ 'temporary' if temporary }} {{ stage_name }}
--         url='{{ s3namespace_stage_name_url }}'
--         credentials=(aws_key_id='{{ env_var("SNOWFLAKE_AWS_ACCESS_KEY_ID") }}' aws_secret_key='{{ env_var("SNOWFLAKE_AWS_SECRET_ACCESS_KEY") }}')
--         file_format = {{ file_format }};
-- {% endmacro %}

{% materialization from_external_stage, adapter='snowflake' -%}
    {%- set identifier = model['alias'] -%}
    --  {%- set stage_name = namespace_stage_name(config.get('stage_name', default=identifier ~ '_stage')) -%}
    {%- set stage_name = '@azureblobdata/fact/' -%}

    -- {%- set stage_url = config.require('stage_url') -%}
    {%- set stage_file_format = config.get('stage_file_format', default='(type = JSON)') -%}
        -- {%- set stage_file_format = '(type = JSON)' -%}
    -- {%- call statement() -%}
    --     {{ ensure_external_stage(stage_name, stage_url, stage_file_format, temporary=False) }}
    -- {%- endcall -%}

    -- {%- set old_relation = adapter.get_relation(schema=schema, identifier=identifier) -%}
    --{%- set target_relation = api.Relation.create(schema=schema, identifier=identifier, type='table') -%}
    {%- set target_relation = 'TRANSACTIONSPERIOD_TEST' -%}
 {%- set namespace_stage_name = 'dummy' -%}
    -- {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    -- {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
    -- {%- set should_drop = (full_refresh_mode or not exists_as_table) -%}

    -- setup
    -- {% if old_relation is none -%}
    --     -- noop
    -- {%- elif should_drop -%}
    --     {{ adapter.drop_relation(old_relation) }}
    --     {%- set old_relation = none -%}
    -- {%- endif %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    -- build model
    -- {% if full_refresh_mode or old_relation is none -%}
    --     {#
    --         -- Create an empty table with columns as specified in sql.
    --         -- We append a unique invocation_id to ensure no files are actually loaded, and an empty row set is returned,
    --         -- which serves as a template to create the table.
    --     #}
    --     {%- call statement() -%}
    --         CREATE OR REPLACE TABLE {{ target_relation }} AS (
    --             {{ sql | replace('__STAGE_TOKEN__', stage_name ~ '/' ~ invocation_id) }}
    --         )
    --     {%- endcall -%}
    -- {%- endif %}

    {# Perform the main load operation using COPY INTO #}
    {%- call statement('main') -%}
        {# TODO: Figure out how to deal with the ordering of columns changing in the model sql... #}
        COPY INTO {{ target_relation }}
        FROM (
            {{ sql | replace('__STAGE_TOKEN__', stage_name)}}
        ) 
 file_format = (type = JSON COMPRESSION =NONE )
 pattern = '.*TRANSACTIONSPERIOD.*json'
    {% endcall %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

{%- endmaterialization %}