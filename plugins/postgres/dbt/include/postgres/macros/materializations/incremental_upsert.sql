

{% macro incremental_upsert_pg(tmp_relation, target_relation, unique_key=none, statement_name="main", created_column=none, conflict_condition=none,upsert_where=none ) %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%} 
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {%- set created_column_list = []  -%}
    {%- set created_column_list =created_column_list.append(created_column)  -%}
    {%- set update_statements = [] -%}


    {%- set column_list = [] -%}
      {%- for column in dest_columns | map(attribute='quoted') -%}
        {%- if column in created_column -%}
        {% else  %}        
            {%- set column_list = column_list.append(column)  -%}  
        {%- endif -%}
    {%- endfor -%}
    {%- for column in column_list -%}
            {%- set update_statements = update_statements.append(column~ '= EXCLUDED.'~column)  -%}  
    {%- endfor -%}
        

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation }}      
    ) on conflict {{conflict_condition}}
    
    DO
    update set     {{ update_statements|join(", ") }}
    
   {% if upsert_where == true%}
    where {{ target_relation }}.record_hash <> EXCLUDED.record_hash
   {%- endif -%}        

    
{%- endmacro %}


{% materialization incremental_pg,default -%}

  {% set unique_key = config.get('unique_key') %}
  {% set conflict_condition = config.get('conflict_condition') %}
  {% set created_column = config.get('created_column') %}
  {% set upsert_where = config.get('upsert_where') %}
  {% set full_refresh_mode = flags.FULL_REFRESH %}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or full_refresh_mode %}
      {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
      {% do adapter.drop_relation(backup_relation) %}

      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
      {% do to_drop.append(backup_relation) %}
  {% else %}
      {% set tmp_relation = make_temp_relation(target_relation) %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}
      {% set build_sql = incremental_upsert_pg(tmp_relation, target_relation, unique_key=unique_key, conflict_condition=conflict_condition, created_column=created_column, upsert_where=upsert_where) %}
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}





