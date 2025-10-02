DROP TABLE IF EXISTS {{ staging_schema }}.{{ table_name }};

CREATE TABLE {{ staging_schema }}.{{ table_name }}
AS SELECT {{ cols }}
FROM {{ prod_schema }}.{{ table_name }}
WITH NO DATA;

COPY {{ staging_schema }}.{{ table_name }} ({{ cols }})
FROM STDIN
DELIMITER ','
CSV HEADER
