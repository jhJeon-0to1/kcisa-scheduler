INSERT INTO PBLPRFR_VIEWNG_CRSTAT
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD, CTPRVN_NM, EXPNDTR_PRICE, VIEWNG_NMPR_CO, SEAT_PER_EXPNDTR_PRICE, POPLTN_PER_VIEWNG_NMPR_CO, THTRE_VIEWNG_NMPR_CO, MUSICL_VIEWNG_NMPR_CO, CLSIC_VIEWNG_NMPR_CO, KCLSIC_VIEWNG_NMPR_CO, POPULAR_MUSIC_VIEWNG_NMPR_CO, DANCE_VIEWNG_NMPR_CO, POPULAR_DANCE_VIEWNG_NMPR_CO, CIRCUS_VIEWNG_NMPR_CO, COMPLEX_VIEWNG_NMPR_CO, METRP_AREA_AT)
SELECT
    BASE_DE
    , SUBSTR(BASE_DE, 1, 4) as BASE_YEAR
    , SUBSTR(BASE_DE, 5, 2) as BASE_MT
    , SUBSTR(BASE_DE, 7, 2) as BASE_DAY
    , CTPRVN_CD AS CTPRVN_CD
    , MAX(CTPRVN_NM) AS CTPRVN_NM
    , SUM(PBLPRFR_SALES_PRICE) * 1000 AS EXPNDTR_PRICE
    , SUM(PBLPRFR_VIEWNG_NMPR_CO) AS VIEWNG_NMPR_CO
    , (CASE SUM(PBLPRFR_VIEWNG_NMPR_CO) WHEN 0 THEN 0 ELSE IFNULL(SUM(PBLPRFR_SALES_PRICE) * 1000 / SUM(PBLPRFR_VIEWNG_NMPR_CO), 0) END) AS SEAT_PER_EXPNDTR_PRICE
    , SUM(PBLPRFR_VIEWNG_NMPR_CO) * 1000 / (
        IFNULL(
                (
                    SELECT
                        POPLTN_CO
                    FROM CTPRVN_ACCTO_POPLTN_INFO AS PP
                    WHERE PP.CTPRVN_CD = B.CTPRVN_CD
                        AND PP.BASE_YM = SUBSTR(B.BASE_DE, 1, 6)
                )
                , (
                SELECT
                    POPLTN_CO
                FROM CTPRVN_ACCTO_POPLTN_INFO AS PP
                WHERE PP.CTPRVN_CD = B.CTPRVN_CD
                    AND PP.BASE_YM = (
                        SELECT
                            MAX(BASE_YM) AS BASE_YM
                        FROM CTPRVN_ACCTO_POPLTN_INFO AS P
                        WHERE P.CTPRVN_CD = B.CTPRVN_CD
                    )
                )
            )
    ) AS POPLTN_PER_VIEWNG_NMPR_CO
    , SUM(IF(GENRE_CD = 'AAAA', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS THTRE_VIEWNG_NMPR_CO
    , SUM(IF(GENRE_CD = 'GGGA', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS MUSICL_VIEWNG_NMPR_CO
    , SUM(IF(GENRE_CD = 'CCCA', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS CLSIC_VIEWNG_NMPR_CO
    , SUM(IF(GENRE_CD = 'CCCC', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS KCLSIC_VIEWNG_NMPR_CO
    , SUM(IF(GENRE_CD = 'CCCD', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS POPULAR_MUSIC_VIEWNG_NMPR_CO
    , SUM(IF(GENRE_CD = 'BBBC', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS DANCE_VIEWNG_NMPR_CO
    , SUM(IF(GENRE_CD = 'BBBE', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS POPULAR_DANCE_VIEWNG_NMPR_CO
    , SUM(IF(GENRE_CD = 'EEEB', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS CIRCUS_VIEWNG_NMPR_CO
    , SUM(IF(GENRE_CD = 'EEEA', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS COMPLEX_VIEWNG_NMPR_CO
    , (SELECT METRP_AT FROM CTPRVN_INFO AS A WHERE A.CTPRVN_CD = B.CTPRVN_CD) AS METRP_AT
FROM colct_pblprfr_viewng_ctprvn_accto_stat AS B
WHERE BASE_DE = ?
GROUP BY BASE_DE, CTPRVN_CD
UNION ALL
SELECT
    BASE_DE
    , SUBSTR(BASE_DE, 1, 4) as BASE_YEAR
    , SUBSTR(BASE_DE, 5, 2) as BASE_MT
    , SUBSTR(BASE_DE, 7, 2) as BASE_DAY
    , '00' AS CTPRVN_CD
    , '전국' AS CTPRVN_NM
    , SUM(PBLPRFR_SALES_PRICE) * 1000 AS EXPNDTR_PRICE
    , SUM(PBLPRFR_VIEWNG_NMPR_CO) AS VIEWNG_NMPR_CO
    , (CASE SUM(PBLPRFR_VIEWNG_NMPR_CO) WHEN 0 THEN 0 ELSE IFNULL(SUM(PBLPRFR_SALES_PRICE) * 1000 / SUM(PBLPRFR_VIEWNG_NMPR_CO), 0) END) AS SEAT_PER_EXPNDTR_PRICE
    , SUM(PBLPRFR_VIEWNG_NMPR_CO) * 1000 / (
        IFNULL(
                (
                    SELECT
                        POPLTN_CO
                    FROM CTPRVN_ACCTO_POPLTN_INFO AS PP
                    WHERE PP.CTPRVN_CD = '00'
                        AND PP.BASE_YM = SUBSTR(B.BASE_DE, 1, 6)
                )
                , (
                SELECT
                    POPLTN_CO
                FROM CTPRVN_ACCTO_POPLTN_INFO AS PP
                WHERE PP.CTPRVN_CD = '00'
                    AND PP.BASE_YM = (
                        SELECT
                            MAX(BASE_YM) AS BASE_YM
                        FROM CTPRVN_ACCTO_POPLTN_INFO AS P
                        WHERE P.CTPRVN_CD = '00'
                    )
                )
            )
    ) AS POPLTN_PER_VIEWNG_NMPR_CO
    , SUM(IF(GENRE_CD = 'AAAA', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS THTRE_RASNG_CUTIN_CO
    , SUM(IF(GENRE_CD = 'GGGA', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS MUSICL_RASNG_CUTIN_CO
    , SUM(IF(GENRE_CD = 'CCCA', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS CLSIC_RASNG_CUTIN_CO
    , SUM(IF(GENRE_CD = 'CCCC', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS KCLSIC_RASNG_CUTIN_CO
    , SUM(IF(GENRE_CD = 'CCCD', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS POPULAR_MUSIC_RASNG_CUTIN_CO
    , SUM(IF(GENRE_CD = 'BBBC', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS DANCE_RASNG_CUTIN_CO
    , SUM(IF(GENRE_CD = 'BBBE', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS POPULAR_DANCE_RASNG_CUTIN_CO
    , SUM(IF(GENRE_CD = 'EEEB', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS CIRCUS_RASNG_CUTIN_CO
    , SUM(IF(GENRE_CD = 'EEEA', PBLPRFR_VIEWNG_NMPR_CO, 0)) AS COMPLEX_RASNG_CUTIN_CO
    , 'N' AS METRP_AT
FROM colct_pblprfr_viewng_ctprvn_accto_stat AS B
WHERE BASE_DE = ?
GROUP BY BASE_DE