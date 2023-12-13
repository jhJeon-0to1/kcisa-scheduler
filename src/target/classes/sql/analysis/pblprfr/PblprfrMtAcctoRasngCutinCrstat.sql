INSERT INTO
	analysis_model.pblprfr_mt_accto_rasng_cutin_crstat
(BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM,
 PBLPRFR_CO, RASNG_CUTIN_CO, RASNG_CUTIN_RT,
 THTRE_RASNG_CUTIN_CO, MUSICL_RASNG_CUTIN_CO,
 CLSIC_RASNG_CUTIN_CO, KCLSIC_RASNG_CUTIN_CO,
 POPULAR_MUSIC_RASNG_CUTIN_CO, DANCE_RASNG_CUTIN_CO,
 POPULAR_DANCE_RASNG_CUTIN_CO, CIRCUS_RASNG_CUTIN_CO,
 COMPLEX_RASNG_CUTIN_CO, METRP_AREA_AT)
SELECT
	BASE_YM
, SUBSTR(BASE_YM, 1, 4)         as BASE_YEAR
, SUBSTR(BASE_YM, 5, 2)         as BASE_MT
, CTPRVN_CD                     AS CTPRVN_CD
, (SELECT CTPRVN_NM
   FROM ctprvn_info
   WHERE
	   B.CTPRVN_CD = CTPRVN_CD)   AS CTPRVN_NM
, SUM(PBLPRFR_CO)               AS PBLPRFR_CO
, SUM(B.RASNG_CUTIN_CO)         AS RASNG_CUTIN_CO
, (CASE SUM(PBLPRFR_CO)
	   WHEN 0 THEN 0
	   ELSE IFNULL(SUM(B.RASNG_CUTIN_CO) /
	               SUM(B.PBLPRFR_CO) * 100,
	               0) END)        AS RASNG_CUTIN_RATE
, SUM(IF(GENRE_CD = 'AAAA', B.RASNG_CUTIN_CO,
         0))                    AS THTRE_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'GGGA', B.RASNG_CUTIN_CO,
         0))                    AS MUSICL_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'CCCA', B.RASNG_CUTIN_CO,
         0))                    AS CLSIC_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'CCCC', B.RASNG_CUTIN_CO,
         0))                    AS KCLSIC_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'CCCD', B.RASNG_CUTIN_CO,
         0))                    AS POPULAR_MUSIC_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'BBBC', B.RASNG_CUTIN_CO,
         0))                    AS DANCE_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'BBBE', B.RASNG_CUTIN_CO,
         0))                    AS POPULAR_DANCE_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'EEEB', B.RASNG_CUTIN_CO,
         0))                    AS CIRCUS_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'EEEA', B.RASNG_CUTIN_CO,
         0))                    AS COMPLEX_RASNG_CUTIN_CO
, (SELECT METRP_AT
   FROM ctprvn_info AS A
   WHERE
	   A.CTPRVN_CD = B.CTPRVN_CD) AS METRP_AT
FROM colct_pblprfr_viewng_mt_accto_ctprvn_accto_stats as B
where
	BASE_YM = ?
GROUP by
	BASE_YM, CTPRVN_CD
UNION ALL
SELECT
	BASE_YM
, SUBSTR(BASE_YM, 1, 4)  as BASE_YEAR
, SUBSTR(BASE_YM, 5, 2)  as BASE_MT
, '00'                   AS CTPRVN_CD
, '전국'                   AS CTPRVN_NM
, SUM(PBLPRFR_CO)        AS PBLPRFR_CO
, SUM(B.RASNG_CUTIN_CO)  AS RASNG_CUTIN_CO
, (CASE SUM(PBLPRFR_CO)
	   WHEN 0 THEN 0
	   ELSE IFNULL(SUM(B.RASNG_CUTIN_CO) /
	               SUM(PBLPRFR_CO) * 100,
	               0) END) AS RASNG_CUTIN_RATE
, SUM(IF(GENRE_CD = 'AAAA', B.RASNG_CUTIN_CO,
         0))             AS THTRE_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'GGGA', B.RASNG_CUTIN_CO,
         0))             AS MUSICL_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'CCCA', B.RASNG_CUTIN_CO,
         0))             AS CLSIC_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'CCCC', B.RASNG_CUTIN_CO,
         0))             AS KCLSIC_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'CCCD', B.RASNG_CUTIN_CO,
         0))             AS POPULAR_MUSIC_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'BBBC', B.RASNG_CUTIN_CO,
         0))             AS DANCE_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'BBBE', B.RASNG_CUTIN_CO,
         0))             AS POPULAR_DANCE_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'EEEB', B.RASNG_CUTIN_CO,
         0))             AS CIRCUS_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'EEEA', B.RASNG_CUTIN_CO,
         0))             AS COMPLEX_RASNG_CUTIN_CO
, 'N'                    AS METRP_AT
FROM colct_pblprfr_viewng_mt_accto_ctprvn_accto_stats as B
WHERE
	BASE_YM = ?
GROUP by
	BASE_YM
ON DUPLICATE KEY UPDATE
	                 BASE_YM                      = VALUES(BASE_YM)
                 , BASE_YEAR                    = VALUES(BASE_YEAR)
                 , BASE_MT                      = VALUES(BASE_MT)
                 , CTPRVN_CD                    = VALUES(CTPRVN_CD)
                 , CTPRVN_NM                    = VALUES(CTPRVN_NM)
                 , PBLPRFR_CO                   = VALUES(PBLPRFR_CO)
                 , RASNG_CUTIN_CO               = VALUES(RASNG_CUTIN_CO)
                 , RASNG_CUTIN_RT               = VALUES(RASNG_CUTIN_RT)
                 , THTRE_RASNG_CUTIN_CO         = VALUES(THTRE_RASNG_CUTIN_CO)
                 , MUSICL_RASNG_CUTIN_CO        = VALUES(MUSICL_RASNG_CUTIN_CO)
                 , CLSIC_RASNG_CUTIN_CO         = VALUES(CLSIC_RASNG_CUTIN_CO)
                 , KCLSIC_RASNG_CUTIN_CO        = VALUES(KCLSIC_RASNG_CUTIN_CO)
                 , POPULAR_MUSIC_RASNG_CUTIN_CO = VALUES(POPULAR_MUSIC_RASNG_CUTIN_CO)
                 , DANCE_RASNG_CUTIN_CO         = VALUES(DANCE_RASNG_CUTIN_CO)
                 , POPULAR_DANCE_RASNG_CUTIN_CO = VALUES(POPULAR_DANCE_RASNG_CUTIN_CO)
                 , CIRCUS_RASNG_CUTIN_CO        = VALUES(CIRCUS_RASNG_CUTIN_CO)
                 , COMPLEX_RASNG_CUTIN_CO       = VALUES(COMPLEX_RASNG_CUTIN_CO)
                 , METRP_AREA_AT                = VALUES(METRP_AREA_AT)