{# Gold view names must never replace physical tables, including with full refresh. #}
{% macro athena__handle_existing_table(full_refresh, old_relation) %}
  {{ exceptions.raise_compiler_error(
      "Refusing to replace a physical table with a Gold view: " ~ old_relation.render()
  ) }}
{% endmacro %}
