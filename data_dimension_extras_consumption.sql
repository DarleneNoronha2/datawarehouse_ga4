

config { 
    type: "view",
    assertions: {
    nonNull: ["id_extras_consumption", "date"],
    uniqueKey: ["id_extras_consumption"]
    }
}

WITH extras_data AS (
    SELECT 
    user_id_new as 
    video_nm AS video_name,
    program_nm AS program_name,
    assinatura AS user_type,
    'Trailers' AS streaming_type,
    date AS date,
    ROW_NUMBER() OVER (PARTITION BY user_id_new) as rn
    FROM `consumo`
    WHERE date = DATE_SUB(CURRENT_DATE(), INTERVAL 3 day)
)

SELECT
    TO_BASE64(SHA256(CONCAT("dec",IFNULL(user_id_new, ""),date,rn))) AS id_extras_consumption,
    * EXCEPT (rn)
FROM 
    extras_data