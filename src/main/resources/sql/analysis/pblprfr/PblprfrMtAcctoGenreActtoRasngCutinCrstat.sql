INSERT INTO
	analysis_model.pblprfr_mt_actto_genre_accto_rasng_cutin_crstat
(BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM,
 GENRE_CD, GENRE_NM, RASNG_CUTIN_CO)
SELECT
	BASE_YM
, SUBSTR(BASE_YM, 1, 4)           AS BASE_YEAR
, SUBSTR(BASE_YM, 5, 2)           AS BASE_MT
, CTPRVN_CD                       AS CTPRVN_CD
, (SELECT CTPRVN_NM
   FROM ctprvn_info
   WHERE
	   LOCAL.CTPRVN_CD = CTPRVN_CD) AS CTPRVN_NM
, GENRE_CD                        AS GENRE_CD
, GENRE_NM                        AS GENRE_NM
, RASNG_CUTIN_CO
FROM colct_pblprfr_viewng_mt_accto_ctprvn_accto_stats AS LOCAL
WHERE
	BASE_YM = ?
UNION ALL
SELECT
	BASE_YM
, SUBSTR(BASE_YM, 1, 4) AS BASE_YEAR
, SUBSTR(BASE_YM, 5, 2) AS BASE_MT
, '00'                  AS CTPRVN_CD
, '전국'                  AS CTPRVN_NM
, GENRE_CD              AS GENRE_CD
, MAX(GENRE_NM)         AS GENRE_NM
, SUM(RASNG_CUTIN_CO)   AS RASNG_CUTIN_CO
FROM colct_pblprfr_viewng_mt_accto_ctprvn_accto_stats
WHERE
	BASE_YM = ?
GROUP by
	BASE_YM, GENRE_CD
ON DUPLICATE KEY UPDATE
	                 BASE_YM        = VALUES(BASE_YM)
                 , BASE_YEAR      = VALUES(BASE_YEAR)
                 , BASE_MT        = VALUES(BASE_MT)
                 , CTPRVN_CD      = VALUES(CTPRVN_CD)
                 , CTPRVN_NM      = VALUES(CTPRVN_NM)
                 , GENRE_CD       = VALUES(GENRE_CD)
                 , GENRE_NM       = VALUES(GENRE_NM)
                 , RASNG_CUTIN_CO = VALUES(RASNG_CUTIN_CO)