-- tests/generic/test_reconcile_aggregates.sql
{% test reconcile_aggregates(
    model,
    compare_relation,
    src_col_express,
    comp_col_express,
    src_group_by=none,
    comp_group_by=none,
    join_keys=[],
    src_where=none,
    comp_where=none,
    tolerance_pct=0.0
) %}

{#- 
    Extrait les alias depuis src_col_express
    - "SUM(x) as total_x"         → "total_x"
    - "produit"                   → "produit"  (pas d'AS, colonne brute = dimension)
    - "CAST(...) AS date"         → "date"
-#}
{% set aliases = [] %}
{% for expr in src_col_express %}
    {% set lower = expr | lower %}
    {% if ' as ' in lower %}
        {% set alias = expr.split(' as ')[-1].split(' AS ')[-1] | trim %}
    {% else %}
        {% set alias = expr | trim %}
    {% endif %}
    {% do aliases.append(alias) %}
{% endfor %}

{#- Measures = tout ce qui n'est pas une join_key -#}
{% set measures = [] %}
{% for a in aliases %}
    {% if a not in join_keys %}{% do measures.append(a) %}{% endif %}
{% endfor %}

with src as (
    select
        {% for expr in src_col_express %}
        {{ expr }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ model }}
    {% if src_where %}where {{ src_where }}{% endif %}
    {% if src_group_by %}group by {{ src_group_by }}{% endif %}
),

comp as (
    select
        {% for expr in comp_col_express %}
        {{ expr }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ compare_relation }}
    {% if comp_where %}where {{ comp_where }}{% endif %}
    {% if comp_group_by %}group by {{ comp_group_by }}{% endif %}
),

{% if join_keys | length == 0 %}

-- ============ MODE SCALAIRE ============
diffs as (
    select
        {% for m in measures %}
        src.{{ m }}  as src_{{ m }},
        comp.{{ m }} as comp_{{ m }},
        abs(coalesce(src.{{ m }}, 0) - coalesce(comp.{{ m }}, 0)) as abs_diff_{{ m }},
        safe_divide(
            abs(coalesce(src.{{ m }}, 0) - coalesce(comp.{{ m }}, 0)),
            nullif(comp.{{ m }}, 0)
        ) as rel_diff_{{ m }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from src cross join comp
)
select * from diffs
where
    {% for m in measures %}
    coalesce(rel_diff_{{ m }}, 0) > {{ tolerance_pct }}
    {% if not loop.last %}or{% endif %}
    {% endfor %}

{% else %}

-- ============ MODE ROW-LEVEL ============
diffs as (
    select
        {% for k in join_keys %}
        coalesce(src.{{ k }}, comp.{{ k }}) as {{ k }},
        {% endfor %}
        case when src.{{ join_keys[0] }}  is null then 1 else 0 end as missing_in_src,
        case when comp.{{ join_keys[0] }} is null then 1 else 0 end as missing_in_comp,
        {% for m in measures %}
        src.{{ m }}  as src_{{ m }},
        comp.{{ m }} as comp_{{ m }},
        abs(coalesce(src.{{ m }}, 0) - coalesce(comp.{{ m }}, 0)) as abs_diff_{{ m }},
        safe_divide(
            abs(coalesce(src.{{ m }}, 0) - coalesce(comp.{{ m }}, 0)),
            nullif(comp.{{ m }}, 0)
        ) as rel_diff_{{ m }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from src
    full outer join comp
        on {% for k in join_keys %}
               src.{{ k }} = comp.{{ k }}
               {% if not loop.last %} and {% endif %}
           {% endfor %}
)
select * from diffs
where
    missing_in_src  = 1
    or missing_in_comp = 1
    {% for m in measures %}
    or coalesce(rel_diff_{{ m }}, 0) > {{ tolerance_pct }}
    {% endfor %}

{% endif %}

{% endtest %}
