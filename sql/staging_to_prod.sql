
{% if table_name in ["artist", "track"] %}
INSERT INTO {{ prod_schema }}.{{ table_name }}
SELECT *, now() AS created
FROM {{ staging_schema }}.{{ table_name }}
ON CONFLICT ({{ primary_keys | join(", ") }}) DO UPDATE SET
    {% for col in update_cols %}
        {{ col }} = EXCLUDED.{{ col }},
    {% endfor %}
    updated = now()
WHERE
    {% for col in update_cols %}
        {{ table_name }}.{{ col }} IS DISTINCT FROM EXCLUDED.{{ col }}{% if not loop.last %} OR {% endif %}
    {% endfor %};

{% elif table_name in ["played", "track_artist"] %}
INSERT INTO {{ prod_schema }}.{{ table_name }}
SELECT *
FROM {{ staging_schema }}.{{ table_name }}
ON CONFLICT DO NOTHING;
{% endif %}
