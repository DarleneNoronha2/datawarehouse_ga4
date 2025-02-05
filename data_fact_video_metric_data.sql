-- manipulate_data_fact


config { 
  type: "view",
  assertions: {
  nonNull: ["id_metrics", "date"],
  uniqueKey: ["id_metrics"]
  }
}

WITH mc AS (
  SELECT
    REPLACE(video_id, ".0", "") AS video_id,
    UDF.GET_SANITIZED_TEXT(headline) AS video_name,
    video_duration,
    video_fully_watched_threshold
  FROM dataset.mc as mc
  WHERE TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY video_id, UDF.GET_SANITIZED_TEXT(headline) ORDER BY dt_partition DESC) = 1
),


consumo_ga AS (
  SELECT
    user_id_new,
    video_id,
    "VOD" AS origin_name,
    LOWER(video_name) AS video_name,
    video_bucket_10,
    video_bucket_25,
    video_bucket_50,
    video_bucket_75,
    video_bucket_90,
    video_bucket_100,
    starts AS video_views,
    video_duration,
    CAST(playtime_seconds AS INT64) AS seconds_watched,
    video_fully_watched_threshold,
    date_dt AS date,
    playtime_seconds / 60 AS minutes_watched,
    playtime_seconds / 3600 AS hours_watched,
  FROM  consumo_ga as ga
  LEFT JOIN mc
  USING (video_id)
  WHERE
     date_dt = DATE_SUB(CURRENT_DATE(), INTERVAL 3 day)
),

consumo_sim AS (
  SELECT
    user_id_new as user_id_new ,
    s.video_id,
    "Live" AS origin_name,
    LOWER(programa_detalhe2) AS video_name,
    null AS video_bucket_10,
    null AS video_bucket_25,
    null AS video_bucket_50,
    null AS video_bucket_75,
    null AS video_bucket_90,
    null AS video_bucket_100,
    plays AS video_views,
    video_duration,
    CAST(playtime_hours*3600 AS INT64) AS seconds_watched,
    video_fully_watched_threshold,
    data_consumo AS date,
    playtime_hours*60 AS minutes_watched,
    playtime_hours AS hours_watched,
  FROM dataset.externo as s
  LEFT JOIN mc AS mc ON LOWER(s.programa_detalhe2) = LOWER(mc.video_name)
  WHERE
    data_consumo = DATE_SUB(CURRENT_DATE(), INTERVAL 3 day)
),

unified_tables AS (
SELECT
  *
FROM
  consumo_sim
UNION ALL
SELECT
  *
FROM
  consumo_ga
),

auxiliar_rn AS (
  SELECT 
  *,
  ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY date) AS rn
  FROM
  unified_tables
)

SELECT 
TO_BASE64(SHA256(CONCAT("fvmd", IFNULL(user_id_new , ""), video_id, CAST(rn AS STRING), date))) AS id_metrics,
* 
EXCEPT (rn)
FROM auxiliar_rn