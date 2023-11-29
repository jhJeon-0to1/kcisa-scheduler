insert into
    analysis_model.movie_year_accto_viewng_crstat
(BASE_YEAR, CTPRVN_CD, CTPRVN_NM,
 MOVIE_ADNC_CO, POPLTN_PER_MOVIE_ADNC_CO, EXPNDTR_PRICE,
 SEAT_PER_EXPNDTR_PRICE, METRP_AREA_AT)
SELECT
    T.BASE_YEAR                       AS BASE_YEAR
  , T.CTPRVN_CD                       AS CTPRVN_CD
  , T.CTPRVN_NM                       AS CTPRVN_NM
  , T.MOVIE_ADNC_CO                   AS ADNC_CO
  , (T.MOVIE_ADNC_CO * 1000 /
     IFNULL(
             (SELECT SUM(POPLTN_CO)
              FROM
                  ctprvn_accto_popltn_info AS PP
              WHERE
                    PP.CTPRVN_CD = T.CTPRVN_CD
                AND PP.BASE_YEAR = T.BASE_YEAR
              GROUP BY PP.CTPRVN_CD, PP.BASE_YEAR)
         , (SELECT SUM(POPLTN_CO)
            FROM
                ctprvn_accto_popltn_info AS PP
            WHERE
                  PP.CTPRVN_CD = T.CTPRVN_CD
              AND PP.BASE_YEAR =
                  (SELECT MAX(P.BASE_YEAR) AS BASE_YEAR
                   FROM
                       ctprvn_accto_popltn_info AS P
                   WHERE
                       P.CTPRVN_CD = T.CTPRVN_CD)
            GROUP BY PP.CTPRVN_CD, PP.BASE_YEAR)
     )
        )                             AS POPULATION_PER_ADNC_CO
  , T.EXPNDTR_PRICE                   AS EXPNDTR_PRICE
  , T.EXPNDTR_PRICE / T.MOVIE_ADNC_CO AS SEAT_PER_EXPNDTR_PRICE
  , (SELECT METRP_AT
     FROM
         ctprvn_info AS P
     WHERE
         T.CTPRVN_CD = P.CTPRVN_CD)   AS METRP_AREA_AT
FROM
    colct_movie_year_accto_ctprvn_accto_stats AS T
where
    BASE_YEAR = ?
UNION ALL
SELECT
    M.BASE_YEAR                       AS BASE_YEAR
  , '00'                              AS CTPRVN_CD
  , '전국'                              AS CTPRVN_NM
  , M.MOVIE_ADNC_CO                   AS ADNC_CO
  , (M.MOVIE_ADNC_CO * 1000 /
     IFNULL(
             (SELECT SUM(POPLTN_CO)
              FROM
                  ctprvn_accto_popltn_info AS PP
              WHERE
                    PP.CTPRVN_CD = '00'
                AND PP.BASE_YEAR = M.BASE_YEAR
              GROUP BY PP.CTPRVN_CD, PP.BASE_YEAR)
         , (SELECT SUM(POPLTN_CO)
            FROM
                ctprvn_accto_popltn_info AS PP
            WHERE
                  PP.CTPRVN_CD = '00'
              AND PP.BASE_YEAR =
                  (SELECT MAX(P.BASE_YEAR) AS BASE_YEAR
                   FROM
                       ctprvn_accto_popltn_info AS P
                   WHERE
                       P.CTPRVN_CD = '00')
            GROUP BY PP.CTPRVN_CD, PP.BASE_YEAR)
     )
        )                             AS POPULATION_PER_ADNC_CO
  , M.EXPNDTR_PRICE                   AS EXPNDTR_PRICE
  , M.EXPNDTR_PRICE / M.MOVIE_ADNC_CO AS SEAT_PER_EXPNDTR_PRICE
  , 'N'                               AS METRP_AREA_AT
FROM
    colct_movie_year_accto_sales_stats AS M
WHERE
    M.BASE_YEAR = ?
ON DUPLICATE KEY UPDATE
                     MOVIE_ADNC_CO            = VALUES(MOVIE_ADNC_CO)
                   , POPLTN_PER_MOVIE_ADNC_CO = VALUES(POPLTN_PER_MOVIE_ADNC_CO)
                   , EXPNDTR_PRICE            = VALUES(EXPNDTR_PRICE)
                   , SEAT_PER_EXPNDTR_PRICE   = VALUES(SEAT_PER_EXPNDTR_PRICE)
                   , METRP_AREA_AT            = VALUES(METRP_AREA_AT)