config { 
  type: "view",
  assertions: {
  nonNull: ["id_consumption", "date"],
  uniqueKey: ["id_consumption"]
  }
}

WITH consumo_ga AS (
  SELECT 
  TO_BASE64(SHA256(CONCAT("dtc",IFNULL(user_id_new, ""),IFNULL(video_id, ""),hour,minute))) AS id_consumption,
  user_id_new,
  video_id,
  CAST(null AS STRING) AS session_id,
  date_dt AS date,
  hour,
  minute,
  null AS user_age,
  CAST(null AS STRING) AS user_gender,
  program_name AS session_name,
  CAST(null AS STRING) AS program_name,
  CAST(null AS STRING) AS program_details,
  CAST(null AS STRING) AS video_name,
  episodio_name AS episode_name,
  sistema_operacional AS operacional_system,
  CAST(null AS STRING) AS consumption_country, 
  regiao_consumo AS consumption_region,
  regiao_cadastro AS registration_region,
  CAST(null AS STRING) AS consumption_city,
  serviceid AS service_id,
  video_type AS streaming_type,
  program_id,
  CASE
  WHEN video_fechado_aberto = "aberto" THEN true
  ELSE false END AS open_video,
  ambiente_consumo AS consumption_environment,
  tv_fabricante AS tv_manufacturer,
  tv_modelo AS tv_model,
  video_dominio AS video_domain,
  codigo_praca AS codigo_praca,
  CAST(null AS STRING) AS tier,
  CAST(null AS STRING) AS dvr,
  seac_user,
  canal AS channel_name
  FROM `consumo_ga`
  WHERE date_dt = DATE_SUB(CURRENT_DATE(), INTERVAL 3 day)
),

consumo_canal AS (
  SELECT 
  TO_BASE64(SHA256(CONCAT("dtc",IF(user_id_new IS NULL, "", user_id_new ),IFNULL(video_id, ""),sessionid))) AS id_consumption,
  user_id_new  AS user_id_new,
  video_id,
  sessionid AS session_id,
  data_consumo AS date,
  null AS hour,
  null AS minute,
  idade AS user_age,
  genero_usuario AS user_gender,
  CAST(null AS STRING) AS session_name,
  programa AS program_name,
  programa_detalhe AS program_details,
  programa_detalhe2 AS video_name,
  CAST(null AS STRING) AS episode_name,
  CAST(null AS STRING) AS operacional_system,
  pais AS consumption_country, 
  estado AS consumption_region,
  CAST(null AS STRING) AS registration_region,
  cidade AS consumption_city,
  CAST(null AS STRING) AS service_id,
  "Live" AS streaming_type,
  program_id,
  CAST(null AS BOOLEAN) AS open_video,
  dispositivo AS consumption_environment,
  CAST(null AS STRING) AS tv_manufacturer,
  CAST(null AS STRING) AS tv_model,
  video_dominio AS video_domain,
  CAST(null AS STRING) AS codigo_praca,
  tier,
  dvr,
  CAST(null AS BOOLEAN) AS seac_user,
  canal AS channel_name
  FROM `consumo_sim`
  WHERE  data_consumo = DATE_SUB(CURRENT_DATE(), INTERVAL 3 day)
),

unified_data AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY date) AS rn 
  FROM consumo_ga
  UNION ALL
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY date) AS rn 
  FROM consumo_canal
),

catalogo AS (
  SELECT
    video_id,
    headline,
    catalog.slug AS video_name_slug,
    type AS video_type,
    content_rating,
    content_rating_criteria,
    release_year,
    genre.name AS genre_name_unnested,
    director.name AS directors_name_unnested,
    released_at,
    technical_specs_resolutions,
    deeplink,
    subset_id,
    video_available_for,
    dt_partition
  FROM `dataset.catalog` AS catalog,
  UNNEST (genres) AS genre,
  UNNEST (directors) AS director
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY REGEXP_REPLACE(video_id, r'\.0', '') ORDER BY dt_partition DESC) = 1   
),

mmcun AS (
  SELECT
    video_id,
    headline,
    video_name_slug,
    video_type,
    content_rating,
    STRING_AGG(DISTINCT content_rating_criteria_unnested) AS content_rating_criteria,
    release_year,
    STRING_AGG(DISTINCT genre_name_unnested) AS genre_name,
    STRING_AGG(DISTINCT directors_name_unnested) AS directors_name,
    released_at,
    STRING_AGG(DISTINCT technical_specs_resolutions_unnested) AS technical_specs_resolutions,
    deeplink,
    subset_id,
    video_available_for,
    dt_partition
  FROM catalogo,
  UNNEST (content_rating_criteria) AS content_rating_criteria_unnested,
  UNNEST (technical_specs_resolutions) AS technical_specs_resolutions_unnested
  GROUP BY 1,2,3,4,5,7,10,12,13,14,15  
)

SELECT 
 TO_BASE64(SHA256(CONCAT("dtc",IFNULL(user_id_new, ""),IFNULL(video_id, ""),IFNULL(session_id, ""),rn))) AS id_consumption,
  user_id_new,
  video_id,
  session_id,
  date,
  hour,
  minute,
  user_age,
  user_gender,
  session_name,
  program_name,
  program_details,
  IF(video_name IS NULL, headline, video_name) AS video_name,
  video_name_slug,
  episode_name,
  operacional_system,
  consumption_country, 
  consumption_region,
  registration_region,
  consumption_city,
  service_id,
  streaming_type,
  video_type,
  program_id,
  open_video,
  consumption_environment,
  tv_manufacturer,
  tv_model,
  video_domain,
  codigo_praca,
  tier,
  dvr,
  seac_user,
  content_rating,
  content_rating_criteria,
  release_year,
  genre_name,
  directors_name,
  released_at,
  channel_name,
  technical_specs_resolutions,
  deeplink,
  subset_id,
  video_available_for
FROM unified_data
LEFT JOIN mmcun
USING (video_id)