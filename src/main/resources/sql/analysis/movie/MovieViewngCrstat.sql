INSERT INTO
    analysis_model.movie_viewng_crstat
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD,
 CTPRVN_NM, MOVIE_ADNC_CO, POPLTN_PER_MOVIE_ADNC_CO,
 EXPNDTR_PRICE, SEAT_PER_EXPNDTR_PRICE, METRP_AREA_AT)
SELECT
    BASE_DE
  , SUBSTR(BASE_DE, 1, 4)                            AS BASE_YEAR
  , SUBSTR(BASE_DE, 5, 2)                            AS BASE_MT
  , SUBSTR(BASE_DE, 7, 2)                            AS BASE_DAY
  , T.CTPRVN_CD
  , T.CTPRVN_NM
  , T.MOVIE_ADNC_CO
  , (T.MOVIE_ADNC_CO * 1000 / (
    IFNULL(
            (SELECT POPLTN_CO
             FROM ctprvn_accto_popltn_info AS PP
             WHERE
                   PP.CTPRVN_CD = T.CTPRVN_CD
               AND PP.BASE_YM = SUBSTR(T.BASE_DE, 1, 6))
        , (SELECT POPLTN_CO
           FROM ctprvn_accto_popltn_info AS PP
           WHERE
                 PP.CTPRVN_CD = T.CTPRVN_CD
             AND PP.BASE_YM =
                 (SELECT MAX(BASE_YM) AS BASE_YM
                  FROM ctprvn_accto_popltn_info AS P
                  WHERE
                      P.CTPRVN_CD = T.CTPRVN_CD))
    )
    ))                                               AS POPULATION_PER_ADNC_CO
  , T.EXPNDTR_PRICE
  , (CASE T.MOVIE_ADNC_CO
         WHEN 0 THEN 0
         ELSE T.EXPNDTR_PRICE / T.MOVIE_ADNC_CO END) AS SEAT_PER_EXPNDTR_PRICE
  , (SELECT METRP_AT
     FROM ctprvn_info AS P
     WHERE
         T.CTPRVN_CD = P.CTPRVN_CD)                  AS METRP_AREA_AT
FROM colct_movie_ctprvn_accto_stats AS T
WHERE
    T.BASE_DE = ?
UNION ALL
SELECT
    M.BASE_DE
  , SUBSTR(M.BASE_DE, 1, 4)                          AS BASE_YEAR
  , SUBSTR(M.BASE_DE, 5, 2)                          AS BASE_MT
  , SUBSTR(M.BASE_DE, 7, 2)                          AS BASE_DAY
  , '00'                                             AS CTPRVN_CD
  , '전국'                                             AS CTPRVN_NM
  , M.MOVIE_ADNC_CO
  , (M.MOVIE_ADNC_CO * 1000 / (
    IFNULL(
            (SELECT POPLTN_CO
             FROM ctprvn_accto_popltn_info AS PP
             WHERE
                   PP.CTPRVN_CD = '00'
               AND PP.BASE_YM = SUBSTR(M.BASE_DE, 1, 6))
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
    ))                                               AS POPULATION_PER_ADNC_CO
  , M.EXPNDTR_PRICE
  , (CASE M.MOVIE_ADNC_CO
         WHEN 0 THEN 0
         ELSE M.EXPNDTR_PRICE / M.MOVIE_ADNC_CO END) AS SEAT_PER_EXPNDTR_PRICE
  , 'N'                                              AS METRP_AREA_AT
FROM colct_movie_sales_stats AS M
WHERE
    M.BASE_DE = ?
ON DUPLICATE KEY UPDATE
                     MOVIE_ADNC_CO            = VALUES(MOVIE_ADNC_CO)
                   , POPLTN_PER_MOVIE_ADNC_CO = VALUES(POPLTN_PER_MOVIE_ADNC_CO)
                   , EXPNDTR_PRICE            = VALUES(EXPNDTR_PRICE)
                   , SEAT_PER_EXPNDTR_PRICE   = VALUES(SEAT_PER_EXPNDTR_PRICE)
                   , METRP_AREA_AT            = VALUES(METRP_AREA_AT)