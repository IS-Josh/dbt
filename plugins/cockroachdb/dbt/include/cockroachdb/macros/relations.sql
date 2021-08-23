{% macro cockroachdb_get_relations () -%}

  {#
      -- in pg_depend, objid is the dependent, refobjid is the referenced object
      --  > a pg_depend entry indicates that the referenced object cannot be
      --  > dropped without also dropping the dependent object.
  #}

  {%- call statement('relations', fetch_result=True) -%}
           
select t.schema_name referenced_schema
,vd.descriptor_name  referenced_name
,t2.schema_name dependent_schema
,fd.descriptor_name dependent_name
from crdb_internal.backward_dependencies  vd
inner join  crdb_internal.forward_dependencies fd
on vd.dependson_id  = fd.descriptor_id
inner join crdb_internal."tables" t
on vd.descriptor_id = t.table_id
inner join crdb_internal."tables" t2
on fd.descriptor_id = t2.table_id


group by referenced_schema, referenced_name, dependent_schema, dependent_name
    order by referenced_schema, referenced_name, dependent_schema, dependent_name;

  {%- endcall -%}

  {{ return(load_result('relations').table) }}
{% endmacro %}
