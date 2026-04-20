-- tests/generic/test_reconcile_aggregates.sql
{% test reconcile_aggregates(
    model,
    compare_relation,
    src_col_express,
    comp_col_express,
    src_filter=none,
    comp_filter=none,
    tolerance_pct=0.0
) %}

{#-
    Compare des agrégats calculés sur deux relations de structures différentes.

    - model             : la relation "src" (le modèle dbt testé)
    - compare_relation  : la relation "comp" (ref() ou source())
    - src_col_express   : liste d'expressions SQL avec alias sur model
    - comp_col_express  : liste d'expressions SQL avec alias sur compare_relation
                          → les alias DOIVENT être identiques entre les deux
    - src_filter/comp_filter : WHERE optionnels
    - tolerance_pct     : écart relatif toléré (0 = égalité stricte)
-#}

with src as (
    select
        {% for expr in src_col_express %}
        {{ expr }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ model }}
    {% if src_filter %}where {{ src_filter }}{% endif %}
),

comp as (
    select
        {% for expr in comp_col_express %}
        {{ expr }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ compare_relation }}
    {% if comp_filter %}where {{ comp_filter }}{% endif %}
),

{#- On extrait les alias à partir du 1er set d'expressions -#}
{% set aliases = [] %}
{% for expr in src_col_express %}
    {% set alias = expr.split(' as ')[-1].split(' AS ')[-1] | trim %}
    {% do aliases.append(alias) %}
{% endfor %}

diffs as (
    select
        {% for alias in aliases %}
        src.{{ alias }}  as src_{{ alias }},
        comp.{{ alias }} as comp_{{ alias }},
        abs(coalesce(src.{{ alias }}, 0) - coalesce(comp.{{ alias }}, 0)) as abs_diff_{{ alias }},
        safe_divide(
            abs(coalesce(src.{{ alias }}, 0) - coalesce(comp.{{ alias }}, 0)),
            nullif(comp.{{ alias }}, 0)
        ) as rel_diff_{{ alias }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from src cross join comp
)

-- Le test fail si au moins une métrique diverge au-delà de la tolérance
select *
from diffs
where
    {% for alias in aliases %}
    rel_diff_{{ alias }} > {{ tolerance_pct }}
    {% if not loop.last %}or{% endif %}
    {% endfor %}

{% endtest %}
