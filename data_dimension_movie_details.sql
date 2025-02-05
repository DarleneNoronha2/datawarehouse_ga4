

config { 
  type: "view",
  assertions: {
  nonNull: ["id_movie_details", "dt_partition"],
  uniqueKey: ["id_movie_details"]
  }
}

WITH mc AS (SELECT 
  REPLACE(video_id, ".0", "") AS video_id,
  headline AS original_headline,
  cat.id ASmc_id,
  cover_landscape,
  cover_landscape_minified ,
  description AS video_description,
  self_rated_content,
  origin_program_id,
  encrypted AS video_encrypted,
  url,
  video_url,
  ARRAY_TO_STRING(countries, ', ') AS countries,
  CAST(
  CASE 
    WHEN archived = "nan" THEN "false" ELSE "true" END
  AS BOOLEAN) AS video_archived,
  format AS video_format,
  logo,
  white_logo,
  poster,
  tv_os_poster4k,
  tv_os_poster_hd,
  cover_portrait,
  cover_wide,
  cover_card,
  cover_poster,
  cover,
  epg_active,
  ARRAY_TO_STRING(genres_ids, ', ') AS genre_id,
  genres.slug AS genre_slug,
  directors.id AS directors_id,
  cast_movie.name AS cast_names,
  cast_movie.id AS cast_id,
  screenwriters_names,
  original_content,
 ARRAY_TO_STRING(video_availability_rules, ', ') AS video_availability_rules,
  service_id,
  channel_id,
  channel_slug,
  subset_headline,
  subset_slug,
  subset_cover,
  structure_origin_program_id,
  structure_title_id,
  structure_type,
  apple_assets_apple_cover_art,
  apple_assets_apple_backdrop_wide,
  apple_assets_apple_backdrop_tall,
  apple_assets_apple_full_color_content_logo,
  apple_assets_apple_single_color_content_logo,
  apple_assets_poster_app,
  ARRAY_TO_STRING(technical_specs_audio_layouts, ', ') AS technical_specs_audio_layouts,
  content_brand_id,
  content_brand_name,
  content_brand_logo,
  content_brand_trimmed_logo,
  origin_video_id,
  video_kind,
  ARRAY_TO_STRING(regions_allowed, ', ') AS regions_allowed,
  __index_level_0__ AS index_level,
  video_title_origin_program_id,
  video_title_encrypted,
  video_title_epg_active,
  video_accessible_offline,
  video_exhibited_at,
  video_scheduled_unpublication_date,
  video_thumbnails_x90,
  video_thumbnails_x216,
  video_thumbnails_x360,
  video_thumbnails_x720,
  video_thumbnails_x1080,
  video_published,
  video_external_reference_tms_id,
  video_external_reference_tms_root_id,
  ARRAY_TO_STRING(video_genre_ids, ', ') AS video_genre_ids,
  video_genres.slug AS video_genres_slug, 
  video_enable_pause_ads,
  video_ad_unit,
  video_ad_custom_data,
  video_subscription_service_id,
  video_subscription_service_name,
  video_subscription_service_default_service_id AS video_subscription_default_service_id,
  dt_partition,
  ROW_NUMBER() OVER (PARTITION BY cat.id) AS rn
FROM `cat` AS cat,
UNNEST (genres) AS genres,
UNNEST (directors) AS directors,
UNNEST (`cast`) AS cast_movie,
UNNEST (video_genres) AS video_genres
WHERE dt_partition = DATE_SUB(CURRENT_DATE(), INTERVAL 3 day)
)

SELECT 
  TO_BASE64(SHA256(CONCAT(video_id,mc_id, dt_partition, rn))) AS id_movie_details,
  * 
FROM 
 mc