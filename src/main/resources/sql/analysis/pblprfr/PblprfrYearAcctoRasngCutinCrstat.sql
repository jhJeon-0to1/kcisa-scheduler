INSERT INTO pblprfr_year_accto_rasng_cutin_crstat
(BASE_YEAR, CTPRVN_CD, CTPRVN_NM, PBLPRFR_CO,
 RASNG_CUTIN_CO, RASNG_CUTIN_RT, THTRE_RASNG_CUTIN_CO,
 MUSICL_RASNG_CUTIN_CO, CLSIC_RASNG_CUTIN_CO,
 KCLSIC_RASNG_CUTIN_CO, POPULAR_MUSIC_RASNG_CUTIN_CO,
 DANCE_RASNG_CUTIN_CO, POPULAR_DANCE_RASNG_CUTIN_CO,
 CIRCUS_RASNG_CUTIN_CO, COMPLEX_RASNG_CUTIN_CO,
 METRP_AREA_AT)
SELECT BASE_YEAR
     , CTPRVN_CD                         AS CTPRVN_CD
     , MAX(CTPRVN_NM)                    AS CTPRVN_NM
     , SUM(PBLPRFR_CO)                   AS PBLPRFR_CO
     , SUM(PBLPRFR_RASNG_CUTIN_CO)       AS RASNG_CUTIN_CO
     , (CASE SUM(PBLPRFR_CO)
            WHEN 0 THEN 0
            ELSE IFNULL(SUM(PBLPRFR_RASNG_CUTIN_CO) /
                        SUM(PBLPRFR_CO) * 100,
                        0) END)          AS RASNG_CUTIN_RATE
     , SUM(IF(GENRE_CD = 'AAAA', PBLPRFR_RASNG_CUTIN_CO,
              0))                        AS THTRE_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'GGGA', PBLPRFR_RASNG_CUTIN_CO,
              0))                        AS MUSICL_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'CCCA', PBLPRFR_RASNG_CUTIN_CO,
              0))                        AS CLSIC_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'CCCC', PBLPRFR_RASNG_CUTIN_CO,
              0))                        AS KCLSIC_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'CCCD', PBLPRFR_RASNG_CUTIN_CO,
              0))                        AS POPULAR_MUSIC_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'BBBC', PBLPRFR_RASNG_CUTIN_CO,
              0))                        AS DANCE_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'BBBE', PBLPRFR_RASNG_CUTIN_CO,
              0))                        AS POPULAR_DANCE_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'EEEB', PBLPRFR_RASNG_CUTIN_CO,
              0))                        AS CIRCUS_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'EEEA', PBLPRFR_RASNG_CUTIN_CO,
              0))                        AS COMPLEX_RASNG_CUTIN_CO
     , (SELECT METRP_AT
        FROM ctprvn_info AS A
        WHERE A.CTPRVN_CD = B.CTPRVN_CD) AS METRP_AT
FROM colct_pblprfr_viewng_year_accto_ctprvn_accto_stats as B
where BASE_YEAR = ?
GROUP by BASE_YEAR, CTPRVN_CD
UNION ALL
SELECT BASE_YEAR
     , '00'                        AS CTPRVN_CD
     , '전국'                        AS CTPRVN_NM
     , SUM(PBLPRFR_CO)             AS PBLPRFR_CO
     , SUM(PBLPRFR_RASNG_CUTIN_CO) AS RASNG_CUTIN_CO
     , (CASE SUM(PBLPRFR_CO)
            WHEN 0 THEN 0
            ELSE IFNULL(SUM(PBLPRFR_RASNG_CUTIN_CO) /
                        SUM(PBLPRFR_CO) * 100,
                        0) END)    AS RASNG_CUTIN_RATE
     , SUM(IF(GENRE_CD = 'AAAA', PBLPRFR_RASNG_CUTIN_CO,
              0))                  AS THTRE_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'GGGA', PBLPRFR_RASNG_CUTIN_CO,
              0))                  AS MUSICL_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'CCCA', PBLPRFR_RASNG_CUTIN_CO,
              0))                  AS CLSIC_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'CCCC', PBLPRFR_RASNG_CUTIN_CO,
              0))                  AS KCLSIC_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'CCCD', PBLPRFR_RASNG_CUTIN_CO,
              0))                  AS POPULAR_MUSIC_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'BBBC', PBLPRFR_RASNG_CUTIN_CO,
              0))                  AS DANCE_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'BBBE', PBLPRFR_RASNG_CUTIN_CO,
              0))                  AS POPULAR_DANCE_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'EEEB', PBLPRFR_RASNG_CUTIN_CO,
              0))                  AS CIRCUS_RASNG_CUTIN_CO
     , SUM(IF(GENRE_CD = 'EEEA', PBLPRFR_RASNG_CUTIN_CO,
              0))                  AS COMPLEX_RASNG_CUTIN_CO
     , 'N'                         AS METRP_AT
FROM colct_pblprfr_viewng_year_accto_ctprvn_accto_stats as B
WHERE BASE_YEAR = ?
GROUP by BASE_YEAR