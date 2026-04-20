{% macro log_test_results_to_monitoring(results) %}
  {% if execute %}

    {% set create_table_query %}
      CREATE TABLE IF NOT EXISTS {{ target.catalog }}.{{ target.schema }}.dbt_test_results (
        test_execution_id STRING,
        test_execution_timestamp TIMESTAMP,
        test_name STRING,
        test_type STRING,
        model_name STRING,
        column_name STRING,
        test_status STRING,
        failures BIGINT,
        severity STRING,
        target_name STRING
      )
    {% endset %}
    {% do run_query(create_table_query) %}

    {% set test_results = results | selectattr('node.resource_type', 'equalto', 'test') | list %}

    {% for test_result in test_results %}
      {% set test_node = test_result.node %}
      {% set test_name = test_node.name %}

      {# Extraction du model_name depuis refs (RefArgs object) #}
      {% set model_name = 'unknown' %}
      {% if test_node.refs and test_node.refs | length > 0 %}
        {% set model_name = test_node.refs[0].name %}
      {% endif %}

      {# Fallback: extraire depuis depends_on.nodes #}
      {% if model_name == 'unknown' and test_node.depends_on and test_node.depends_on.nodes %}
        {% for dep in test_node.depends_on.nodes %}
          {% if 'model.' in dep %}
            {% set model_name = dep.split('.')[-1] %}
            {% break %}
          {% endif %}
        {% endfor %}
      {% endif %}

      {% set column_name = test_node.column_name if test_node.column_name else 'N/A' %}
      {% set test_status = test_result.status %}
      {% set failures = test_result.failures if test_result.failures is not none else 0 %}
      {% set severity = test_node.config.severity if test_node.config.severity else 'warn' %}

      {% set test_type = 'custom' %}
      {% if 'not_null' in test_name %}
        {% set test_type = 'not_null' %}
      {% elif 'unique' in test_name %}
        {% set test_type = 'unique' %}
      {% elif 'accepted_values' in test_name %}
        {% set test_type = 'accepted_values' %}
      {% elif 'relationships' in test_name %}
        {% set test_type = 'relationships' %}
      {% elif 'not_empty_string' in test_name %}
        {% set test_type = 'not_empty_string' %}
      {% elif 'expect_column_value_lengths' in test_name %}
        {% set test_type = 'value_length' %}
      {% endif %}

      {% if test_type != 'custom' %}

        {% set insert_query %}
          INSERT INTO {{ target.catalog }}.{{ target.schema }}.dbt_test_results
          VALUES (
            '{{ invocation_id }}',
            TIMESTAMP('{{ run_started_at }}'),
            '{{ test_name | replace("'", "''") }}',
            '{{ test_type }}',
            '{{ model_name }}',
            '{{ column_name }}',
            '{{ test_status }}',
            {{ failures }},
            '{{ severity }}',
            '{{ target.name }}'
          )
        {% endset %}

        {% do run_query(insert_query) %}

      {% endif %}

    {% endfor %}

    {{ log("Logged " ~ (test_results | selectattr('node.name', 'defined') | list | length) ~ " test results", info=True) }}

  {% endif %}
{% endmacro %}

    on-run-end:
  - "{% if flags.WHICH == 'test' %}{{ log_test_results_to_monitoring(results) }}{% endif %}"


    
