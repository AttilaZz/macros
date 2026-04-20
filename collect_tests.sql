{#-
    =========================================================================
    log_test_results
    =========================================================================

    Macro à appeler depuis un on-run-end hook. À la fin de dbt test / dbt build,
    parcourt l'objet `results` (injecté par dbt), filtre les tests, et
    INSÈRE une ligne par test dans une table d'audit BigQuery.

    Usage dans dbt_project.yml :
        on-run-end:
          - "{{ log_test_results() }}"

    Paramètres
    ----------
    target_database : projet BQ cible (défaut: target.database)
    target_schema   : dataset BQ cible (défaut: 'dbt_audit')
    target_table    : nom de table (défaut: 'test_results')
    create_if_not_exists : crée la table au premier run si elle n'existe pas
-#}

{% macro log_test_results(
    target_database=none,
    target_schema='dbt_audit',
    target_table='test_results',
    create_if_not_exists=true
) %}

    {#- Skip pendant le parse / compile / quand il n'y a pas de résultats -#}
    {% if not execute %}
        {% do return('') %}
    {% endif %}

    {% if results is not defined or results | length == 0 %}
        {% do log("log_test_results: aucun résultat à logger, skip.", info=true) %}
        {% do return('') %}
    {% endif %}

    {#- Résolution de la destination -#}
    {% set db = target_database or target.database %}
    {% set fq_table = adapter.quote(db) ~ '.' ~ adapter.quote(target_schema) ~ '.' ~ adapter.quote(target_table) %}

    {#- Création de la table si besoin -#}
    {% if create_if_not_exists %}
        {% set create_sql %}
            create schema if not exists {{ adapter.quote(db) }}.{{ adapter.quote(target_schema) }}
            options(location = '{{ target.location or "EU" }}');

            create table if not exists {{ fq_table }} (
                invocation_id     string    not null,
                run_started_at    timestamp not null,
                executed_at       timestamp not null,
                project_name      string,
                target_name       string,
                test_name         string    not null,
                test_unique_id    string,
                resource_type     string,
                status            string    not null,
                failures          int64,
                execution_time_s  float64,
                message           string,
                tags              array<string>,
                severity          string,
                compiled_code     string
            )
            partition by date(executed_at)
            cluster by status, test_name
            options(
                description = 'Journal d''exécution des tests dbt - alimenté par log_test_results()'
            );
        {% endset %}

        {% do run_query(create_sql) %}
    {% endif %}

    {#- Filtrage : uniquement les tests (pas les runs/seeds/snapshots) -#}
    {% set test_results = [] %}
    {% for res in results %}
        {% if res.node.resource_type == 'test' %}
            {% do test_results.append(res) %}
        {% endif %}
    {% endfor %}

    {% if test_results | length == 0 %}
        {% do log("log_test_results: aucun test dans ce run, skip.", info=true) %}
        {% do return('') %}
    {% endif %}

    {#- Construction de l'INSERT -#}
    {% set insert_sql %}
        insert into {{ fq_table }} (
            invocation_id, run_started_at, executed_at,
            project_name, target_name,
            test_name, test_unique_id, resource_type,
            status, failures, execution_time_s, message,
            tags, severity, compiled_code
        )
        values
        {% for res in test_results %}
            (
                {{ dbt_string_literal(invocation_id) }},
                timestamp({{ dbt_string_literal(run_started_at) }}),
                current_timestamp(),
                {{ dbt_string_literal(project_name) }},
                {{ dbt_string_literal(target.name) }},
                {{ dbt_string_literal(res.node.name) }},
                {{ dbt_string_literal(res.node.unique_id) }},
                {{ dbt_string_literal(res.node.resource_type) }},
                {{ dbt_string_literal(res.status) }},
                {{ res.failures if res.failures is not none else 'null' }},
                {{ res.execution_time if res.execution_time is not none else 'null' }},
                {{ dbt_string_literal((res.message or '') | replace("'", "''") | truncate(2000, true, '')) }},
                {{ _tags_to_bq_array(res.node.tags) }},
                {{ dbt_string_literal(res.node.config.severity if res.node.config.severity is defined else 'error') }},
                {{ dbt_string_literal((res.node.compiled_code or '') | replace("'", "''") | truncate(5000, true, '')) if res.node.compiled_code is defined else 'null' }}
            ){% if not loop.last %},{% endif %}
        {% endfor %}
    {% endset %}

    {% do run_query(insert_sql) %}

    {#- Log console pour feedback -#}
    {% set pass_count = 0 %}
    {% set fail_count = 0 %}
    {% set warn_count = 0 %}
    {% set error_count = 0 %}
    {% for r in test_results %}
        {% if r.status == 'pass'  %}{% set pass_count  = pass_count  + 1 %}{% endif %}
        {% if r.status == 'fail'  %}{% set fail_count  = fail_count  + 1 %}{% endif %}
        {% if r.status == 'warn'  %}{% set warn_count  = warn_count  + 1 %}{% endif %}
        {% if r.status == 'error' %}{% set error_count = error_count + 1 %}{% endif %}
    {% endfor %}

    {% do log(
        "log_test_results: " ~ (test_results | length) ~ " test(s) loggés dans " ~ fq_table ~
        " (pass=" ~ pass_count ~ ", fail=" ~ fail_count ~
        ", warn=" ~ warn_count ~ ", error=" ~ error_count ~ ")",
        info=true
    ) %}

{% endmacro %}


{#-
    Convertit une liste de tags Python en littéral BigQuery ARRAY<STRING>.
    Utilisé en interne par log_test_results.
-#}
{% macro _tags_to_bq_array(tags) %}
    {%- if tags is none or tags | length == 0 -%}
        cast(null as array<string>)
    {%- else -%}
        [{% for t in tags %}{{ dbt_string_literal(t) }}{% if not loop.last %}, {% endif %}{% endfor %}]
    {%- endif -%}
{% endmacro %}


# =============================================================================
# Ajout à ton dbt_project.yml existant
# =============================================================================

on-run-end:
  - "{{ log_test_results() }}"

# Ou avec paramètres custom :
# on-run-end:
#   - "{{ log_test_results(target_schema='data_quality', target_table='dbt_test_log') }}"


# -----------------------------------------------------------------------------
# Optionnel : variables pour surcharger par environnement
# -----------------------------------------------------------------------------
vars:
  # Désactive le logging en dev local si besoin
  enable_test_logging: true


# -----------------------------------------------------------------------------
# Alternative : ne logger qu'en prod/CI
# -----------------------------------------------------------------------------
# on-run-end:
#   - "{% if target.name in ['prod', 'ci'] %}{{ log_test_results() }}{% endif %}"




          


          
