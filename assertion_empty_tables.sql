config { type: "assertion" }

WITH all_tables AS (
  SELECT DISTINCT
    "dimension_exc" AS table_name,
    count(1) AS number_of_rows
  FROM 
    ${ref("manipulate_dimension_exc")}
  UNION ALL
  SELECT DISTINCT
    "dimension_md" AS table_name,
    count(1) AS number_of_rows
  FROM 
    ${ref("manipulate_dimension_md")}
  UNION ALL
  SELECT DISTINCT
    "dimension_consumption" AS table_name,
    count(1) AS number_of_rows
  FROM 
    ${ref("manipulate_dimension_consumption")}
  UNION ALL
  SELECT DISTINCT
    "fact_video_metric_data" AS table_name,
    count(1) AS number_of_rows
  FROM 
    ${ref("manipulate_data_fact_video_metric_data")}
)
SELECT
  *
FROM
  all_tables
WHERE number_of_rows = 0