INSERT INTO
	analysis_model.pblprfr_viewng_crstat
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD,
 CTPRVN_NM, EXPNDTR_PRICE, VIEWNG_NMPR_CO,
 SEAT_PER_EXPNDTR_PRICE, POPLTN_PER_VIEWNG_NMPR_CO,
 THTRE_VIEWNG_NMPR_CO, MUSICL_VIEWNG_NMPR_CO,
 CLSIC_VIEWNG_NMPR_CO, KCLSIC_VIEWNG_NMPR_CO,
 POPULAR_MUSIC_VIEWNG_NMPR_CO, DANCE_VIEWNG_NMPR_CO,
 POPULAR_DANCE_VIEWNG_NMPR_CO, CIRCUS_VIEWNG_NMPR_CO,
 COMPLEX_VIEWNG_NMPR_CO, METRP_AREA_AT)
SELECT
	BASE_DE
, SUBSTR(BASE_DE, 1, 4)         as BASE_YEAR
, SUBSTR(BASE_DE, 5, 2)         as BASE_MT
, SUBSTR(BASE_DE, 7, 2)         as BASE_DAY
, CTPRVN_CD                     AS CTPRVN_CD
, (SELECT CTPRVN_NM
   FROM ctprvn_info
   WHERE
	   B.CTPRVN_CD = CTPRVN_CD)   AS CTPRVN_NM
, SUM(B.EXPNDTR_PRICE) * 1000   AS EXPNDTR_PRICE
, SUM(B.VIEWNG_NMPR_CO)         AS VIEWNG_NMPR_CO
, (CASE SUM(B.VIEWNG_NMPR_CO)
	   WHEN 0 THEN 0
	   ELSE IFNULL(SUM(B.EXPNDTR_PRICE) * 1000 /
	               SUM(B.VIEWNG_NMPR_CO),
	               0) END)        AS SEAT_PER_EXPNDTR_PRICE
, SUM(B.VIEWNG_NMPR_CO) * 1000 / (
	IFNULL(
			(SELECT POPLTN_CO
			 FROM ctprvn_accto_popltn_info AS PP
			 WHERE
				   PP.CTPRVN_CD = B.CTPRVN_CD
			 AND PP.BASE_YM = SUBSTR(B.BASE_DE, 1, 6))
		, (SELECT POPLTN_CO
		   FROM ctprvn_accto_popltn_info AS PP
		   WHERE
			     PP.CTPRVN_CD = B.CTPRVN_CD
		   AND PP.BASE_YM =
		       (SELECT MAX(BASE_YM) AS BASE_YM
		        FROM ctprvn_accto_popltn_info AS P
		        WHERE
			        P.CTPRVN_CD = B.CTPRVN_CD))
	)
	)                             AS POPLTN_PER_VIEWNG_NMPR_CO
, SUM(IF(GENRE_CD = 'AAAA', VIEWNG_NMPR_CO,
         0))                    AS THTRE_VIEWNG_NMPR_CO
, SUM(IF(GENRE_CD = 'GGGA', VIEWNG_NMPR_CO,
         0))                    AS MUSICL_VIEWNG_NMPR_CO
, SUM(IF(GENRE_CD = 'CCCA', VIEWNG_NMPR_CO,
         0))                    AS CLSIC_VIEWNG_NMPR_CO
, SUM(IF(GENRE_CD = 'CCCC', VIEWNG_NMPR_CO,
         0))                    AS KCLSIC_VIEWNG_NMPR_CO
, SUM(IF(GENRE_CD = 'CCCD', VIEWNG_NMPR_CO,
         0))                    AS POPULAR_MUSIC_VIEWNG_NMPR_CO
, SUM(IF(GENRE_CD = 'BBBC', VIEWNG_NMPR_CO,
         0))                    AS DANCE_VIEWNG_NMPR_CO
, SUM(IF(GENRE_CD = 'BBBE', VIEWNG_NMPR_CO,
         0))                    AS POPULAR_DANCE_VIEWNG_NMPR_CO
, SUM(IF(GENRE_CD = 'EEEB', VIEWNG_NMPR_CO,
         0))                    AS CIRCUS_VIEWNG_NMPR_CO
, SUM(IF(GENRE_CD = 'EEEA', VIEWNG_NMPR_CO,
         0))                    AS COMPLEX_VIEWNG_NMPR_CO
, (SELECT METRP_AT
   FROM ctprvn_info AS A
   WHERE
	   A.CTPRVN_CD = B.CTPRVN_CD) AS METRP_AT
FROM colct_pblprfr_viewng_ctprvn_accto_stats AS B
WHERE
	BASE_DE = ?
GROUP BY
	BASE_DE, CTPRVN_CD
UNION ALL
SELECT
	BASE_DE
, SUBSTR(BASE_DE, 1, 4)       as BASE_YEAR
, SUBSTR(BASE_DE, 5, 2)       as BASE_MT
, SUBSTR(BASE_DE, 7, 2)       as BASE_DAY
, '00'                        AS CTPRVN_CD
, '전국'                        AS CTPRVN_NM
, SUM(B.EXPNDTR_PRICE) * 1000 AS EXPNDTR_PRICE
, SUM(VIEWNG_NMPR_CO)         AS VIEWNG_NMPR_CO
, (CASE SUM(VIEWNG_NMPR_CO)
	   WHEN 0 THEN 0
	   ELSE IFNULL(SUM(B.EXPNDTR_PRICE) * 1000 /
	               SUM(VIEWNG_NMPR_CO),
	               0) END)      AS SEAT_PER_EXPNDTR_PRICE
, SUM(VIEWNG_NMPR_CO) * 1000 / (
	IFNULL(
			(SELECT POPLTN_CO
			 FROM ctprvn_accto_popltn_info AS PP
			 WHERE
				   PP.CTPRVN_CD = '00'
			 AND PP.BASE_YM = SUBSTR(B.BASE_DE, 1, 6))
		, (SELECT POPLTN_CO
		   FROM ctprvn_accto_popltn_info AS PP
		   WHERE
			     PP.CTPRVN_CD = '00'
		   AND PP.BASE_YM =
		       (SELECT MAX(BASE_YM) AS BASE_YM
		        FROM ctprvn_accto_popltn_info AS P
		        WHERE
			        P.CTPRVN_CD = '00'))
	)
	)                           AS POPLTN_PER_VIEWNG_NMPR_CO
, SUM(IF(GENRE_CD = 'AAAA', VIEWNG_NMPR_CO,
         0))                  AS THTRE_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'GGGA', VIEWNG_NMPR_CO,
         0))                  AS MUSICL_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'CCCA', VIEWNG_NMPR_CO,
         0))                  AS CLSIC_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'CCCC', VIEWNG_NMPR_CO,
         0))                  AS KCLSIC_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'CCCD', VIEWNG_NMPR_CO,
         0))                  AS POPULAR_MUSIC_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'BBBC', VIEWNG_NMPR_CO,
         0))                  AS DANCE_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'BBBE', VIEWNG_NMPR_CO,
         0))                  AS POPULAR_DANCE_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'EEEB', VIEWNG_NMPR_CO,
         0))                  AS CIRCUS_RASNG_CUTIN_CO
, SUM(IF(GENRE_CD = 'EEEA', VIEWNG_NMPR_CO,
         0))                  AS COMPLEX_RASNG_CUTIN_CO
, 'N'                         AS METRP_AT
FROM colct_pblprfr_viewng_ctprvn_accto_stats AS B
WHERE
	BASE_DE = ?
GROUP BY
	BASE_DE
ON DUPLICATE KEY UPDATE
	                 BASE_DE                      = VALUES(BASE_DE)
                 , BASE_YEAR                    = VALUES(BASE_YEAR)
                 , BASE_MT                      = VALUES(BASE_MT)
                 , BASE_DAY                     = VALUES(BASE_DAY)
                 , CTPRVN_CD                    = VALUES(CTPRVN_CD)
                 , CTPRVN_NM                    = VALUES(CTPRVN_NM)
                 , EXPNDTR_PRICE                = VALUES(EXPNDTR_PRICE)
                 , VIEWNG_NMPR_CO               = VALUES(VIEWNG_NMPR_CO)
                 , SEAT_PER_EXPNDTR_PRICE       = VALUES(SEAT_PER_EXPNDTR_PRICE)
                 , POPLTN_PER_VIEWNG_NMPR_CO    = VALUES(POPLTN_PER_VIEWNG_NMPR_CO)
                 , THTRE_VIEWNG_NMPR_CO         = VALUES(THTRE_VIEWNG_NMPR_CO)
                 , MUSICL_VIEWNG_NMPR_CO        = VALUES(MUSICL_VIEWNG_NMPR_CO)
                 , CLSIC_VIEWNG_NMPR_CO         = VALUES(CLSIC_VIEWNG_NMPR_CO)
                 , KCLSIC_VIEWNG_NMPR_CO        = VALUES(KCLSIC_VIEWNG_NMPR_CO)
                 , POPULAR_MUSIC_VIEWNG_NMPR_CO = VALUES(POPULAR_MUSIC_VIEWNG_NMPR_CO)
                 , DANCE_VIEWNG_NMPR_CO         = VALUES(DANCE_VIEWNG_NMPR_CO)
                 , POPULAR_DANCE_VIEWNG_NMPR_CO = VALUES(POPULAR_DANCE_VIEWNG_NMPR_CO)
                 , CIRCUS_VIEWNG_NMPR_CO        = VALUES(CIRCUS_VIEWNG_NMPR_CO)
                 , COMPLEX_VIEWNG_NMPR_CO       = VALUES(COMPLEX_VIEWNG_NMPR_CO)
                 , METRP_AREA_AT                = VALUES(METRP_AREA_AT)