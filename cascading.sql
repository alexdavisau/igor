WITH mismatched_tables AS (
  -- Find all non-deleted tables where data_product is distinct from the parent schema's.
  SELECT
    'table' AS object_type,
    -- ****** FIX: Use "schema_id" from the schema table ******
    s."schema_id" AS schema_id,
    s.name AS schema_name,
    t.id AS object_id_to_update,
    t.name AS object_name,
    s.data_product AS correct_data_product_value,
    t.data_product AS current_data_product_value
  FROM
    public.rdbms_schemas AS s
    -- ****** FIX: Join on "schema_id" = "schema_id" ******
    JOIN public.rdbms_tables AS t ON s."schema_id" = t."schema_id"
  WHERE
    -- 1. Schema has a data product
    s.data_product IS NOT NULL AND cardinality(s.data_product) > 0
    -- 2. Table's data product is mismatched (or NULL)
    AND t.data_product IS DISTINCT FROM s.data_product
    -- 3. Table is not deleted
    AND t.deleted IS NOT TRUE
),
mismatched_columns AS (
  -- Find all non-deleted columns where data_product is distinct from the parent schema's.
  SELECT
    'column' AS object_type,
    -- ****** FIX: Use "schema_id" from the schema table ******
    s."schema_id" AS schema_id,
    s.name AS schema_name,
    c.id AS object_id_to_update,
    t.name || '.' || c.name AS object_name,
    s.data_product AS correct_data_product_value,
    c.data_product AS current_data_product_value
  FROM
    public.rdbms_schemas AS s
    -- ****** FIX: Join on "schema_id" = "schema_id" ******
    JOIN public.rdbms_tables AS t ON s."schema_id" = t."schema_id"
    -- This join is correct based on your prior feedback
    JOIN public.rdbms_columns AS c ON t.table_id = c.table_id
  WHERE
    -- 1. Schema has a data product
    s.data_product IS NOT NULL AND cardinality(s.data_product) > 0
    -- 2. Column's data product is mismatched (or NULL)
    AND c.data_product IS DISTINCT FROM s.data_product
    -- 3. Table and Column are not deleted
    AND t.deleted IS NOT TRUE
    AND c.deleted IS NOT TRUE
)
-- Combine the two lists
SELECT
  object_type,
  schema_id,
  schema_name,
  object_name,
  object_id_to_update,
  correct_data_product_value,
  current_data_product_value
FROM
  mismatched_tables
UNION ALL
SELECT
  object_type,
  schema_id,
  schema_name,
  object_name,
  object_id_to_update,
  correct_data_product_value,
  current_data_product_value
FROM
  mismatched_columns
ORDER BY
  schema_name,
  object_name;
