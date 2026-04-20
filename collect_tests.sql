{#-
    =========================================================================
    log_test_results_to_monitoring  --  version corrigée
    =========================================================================
    Fixes appliqués :
      [1] target.database (BQ) au lieu de target.catalog (Databricks)
      [2] Pas de paramètre 'results' - variable globale injectée par le hook
      [3] TOUS les tests loggés (custom inclus), pas seulement les generic
      [4] test_metadata.name pour détecter proprement le type de test
      [5] Gestion des tests sur sources (pas que sur models)
      [6] Backticks pour sécuriser les project_id avec tiret
      [7] INSERT multi-rows unique au lieu de N INSERTs en boucle
-#}

{% macro log_test_results_to_monitoring() %}
    {% if not execute %}{% do return('') %}{% endif %}

    {% if results is not defined or results | length == 0 %}
        {% do return('') %}
    {% endif %}

    {#- [1][6] FQ name avec backticks et target.database (BQ) -#}
    {% set fq_table %}`{{ target.database }}`.`{{ target.schema }}`.`dbt_test_results`{% endset %}

    {#- Création table -#}
    {% set create_table_query %}
        CREATE TABLE IF NOT EXISTS {{ fq_table }} (
            test_execution_id         STRING,
            test_execution_timestamp  TIMESTAMP,
            test_name                 STRING,
            test_type                 STRING,
            attached_node             STRING,
            attached_node_type        STRING,
            column_name               STRING,
            test_status               STRING,
            failures                  INT64,
            execution_time_s          FLOAT64,
            severity                  STRING,
            target_name               STRING,
            message                   STRING,
            tags                      ARRAY<STRING>
        )
        PARTITION BY DATE(test_execution_timestamp)
        CLUSTER BY test_status, test_name
    {% endset %}
    {% do run_query(create_table_query) %}

    {#- Filtrage tests -#}
    {% set test_results = results | selectattr('node.resource_type', 'equalto', 'test') | list %}

    {% if test_results | length == 0 %}
        {% do log("[log_test_results] Aucun test dans ce run.", info=true) %}
        {% do return('') %}
    {% endif %}

    {#- [7] Construction d'UN SEUL INSERT multi-rows -#}
    {% set rows = [] %}

    {% for test_result in test_results %}
        {% set test_node = test_result.node %}
        {% set test_name = test_node.name %}

        {#- [4] test_type via test_metadata (fiable) -#}
        {% set test_type = 'custom' %}
        {% if test_node.test_metadata is defined and test_node.test_metadata %}
            {% set test_type = test_node.test_metadata.name %}
        {% endif %}

        {#- [5] Détection de ce à quoi le test est attaché (model OU source) -#}
        {% set attached_node = 'unknown' %}
        {% set attached_type = 'unknown' %}
        {% if test_node.depends_on and test_node.depends_on.nodes %}
            {% for dep in test_node.depends_on.nodes %}
                {% if dep.startswith('model.') and attached_type == 'unknown' %}
                    {% set attached_node = dep.split('.')[-1] %}
                    {% set attached_type = 'model' %}
                {% elif dep.startswith('source.') and attached_type == 'unknown' %}
                    {% set parts = dep.split('.') %}
                    {% set attached_node = parts[-2] ~ '.' ~ parts[-1] %}
                    {% set attached_type = 'source' %}
                {% endif %}
            {% endfor %}
        {% endif %}

        {% set column_name = test_node.column_name if test_node.column_name else 'N/A' %}
        {% set test_status = test_result.status %}
        {% set failures = test_result.failures if test_result.failures is not none else 0 %}
        {% set exec_time = test_result.execution_time if test_result.execution_time is not none else 0 %}
        {% set severity = test_node.config.severity if test_node.config.severity else 'warn' %}
        {% set message = (test_result.message or '') | replace("'", "\\'") | replace("\n", " ") | truncate(1500, true, '') %}

        {#- Tags en array BQ -#}
        {% if test_node.tags and test_node.tags | length > 0 %}
            {% set tags_sql %}[{% for t in test_node.tags %}'{{ t }}'{% if not loop.last %}, {% endif %}{% endfor %}]{% endset %}
        {% else %}
            {% set tags_sql = 'CAST(NULL AS ARRAY<STRING>)' %}
        {% endif %}

        {% set row %}
            (
                '{{ invocation_id }}',
                TIMESTAMP('{{ run_started_at }}'),
                '{{ test_name | replace("'", "\\'") }}',
                '{{ test_type }}',
                '{{ attached_node }}',
                '{{ attached_type }}',
                '{{ column_name }}',
                '{{ test_status }}',
                {{ failures }},
                {{ exec_time }},
                '{{ severity }}',
                '{{ target.name }}',
                '{{ message }}',
                {{ tags_sql }}
            )
        {% endset %}

        {% do rows.append(row) %}
    {% endfor %}

    {#- [3] Log TOUS les tests, pas de filtre test_type -#}
    {% set insert_query %}
        INSERT INTO {{ fq_table }} (
            test_execution_id, test_execution_timestamp,
            test_name, test_type,
            attached_node, attached_node_type,
            column_name, test_status,
            failures, execution_time_s,
            severity, target_name,
            message, tags
        )
        VALUES
        {{ rows | join(',\n') }}
    {% endset %}

    {% do run_query(insert_query) %}

    {% do log("[log_test_results] ✓ " ~ (test_results | length) ~ " test(s) loggés dans " ~ fq_table, info=true) %}

{% endmacro %}
