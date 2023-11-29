INSERT INTO
    analysis_model.movie_activate_crstat
(BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM,
 SCRNG_MOVIE_CO, MOVIE_ADNC_CO, EXPNDTR_PRICE,
 POPLTN_PER_MOVIE_ADNC_CO, MOVIE_ADNC_CO_SCORE,
 EXPNDTR_PRICE_SCORE, GNRLZ_SCORE, STDR_SCRNG_MOVIE_CO,
 STDR_MOVIE_ADNC_CO, STDR_EXPNDTR_PRICE)
SELECT
    DATA.BASE_YM                AS BASE_YM
  , DATA.BASE_YEAR              AS BASE_YEAR
  , DATA.BASE_MT                AS BASE_MT
  , DATA.CTPRVN_CD              AS CTPRVN_CD
  , DATA.CTPRVN_NM              AS CTPRVN_NM
  , DATA.SCRNG_MOVIE_CO         AS SCRNG_MOVIE_CO
  , DATA.MOVIE_ADNC_CO          AS MOVIE_ADNC_CO
  , DATA.EXPNDTR_PRICE          AS EXPNDTR_PRICE
  , DATA.POPULATION_PER_ADNC_CO AS POPLTN_PER_MOVIE_ADNC_CO
  , data.POPULATION_PER_ADNC_CO /
    STD.STD_POPLTN_PER_ADNC_CO *
    100                         as MOVIE_ADNC_CO_SCORE
  , data.EXPNDTR_PRICE / data.POP / STD.STD_PRICE *
    100                         as EXPNDTR_PRICE_SCORE
  , ((data.POPULATION_PER_ADNC_CO /
      STD.STD_POPLTN_PER_ADNC_CO * 100) +
     (data.EXPNDTR_PRICE / data.POP / STD.STD_PRICE *
      100)) / 2                 AS GNRLZ_SCORE
  , STD.STRD_SCRNG_MOVIE_CO     AS STDR_SCRNG_MOVIE_CO
  , STD.ADNC_CO_SUM             AS STDR_MOVIE_ADNC_CO
  , STD.EXPNDTR_PRICE_SUM       AS STDR_EXPNDTR_PRICE
FROM
    (SELECT
         T.BASE_YM
       , T.BASE_YEAR
       , T.BASE_MT
       , T.CTPRVN_CD
       , T.CTPRVN_NM
       , T.SCRNG_MOVIE_CO
       , T.MOVIE_ADNC_CO
       , T.EXPNDTR_PRICE
       , (T.MOVIE_ADNC_CO * 1000 /
          IFNULL(
                  (SELECT
                       POPLTN_CO
                   FROM
                       ctprvn_accto_popltn_info AS PP
                   WHERE
                         PP.CTPRVN_CD = T.CTPRVN_CD
                     AND PP.BASE_YM = T.BASE_YM)
              , (SELECT
                     POPLTN_CO
                 FROM
                     ctprvn_accto_popltn_info AS PP
                 WHERE
                       PP.CTPRVN_CD = T.CTPRVN_CD
                   AND PP.BASE_YM = (SELECT
                                         MAX(BASE_YM) AS BASE_YM
                                     FROM
                                         ctprvn_accto_popltn_info AS P
                                     WHERE
                                         P.CTPRVN_CD = T.CTPRVN_CD))
          )
             ) AS POPULATION_PER_ADNC_CO
       , (IFNULL(
                (SELECT
                     POPLTN_CO
                 FROM
                     ctprvn_accto_popltn_info AS PP
                 WHERE
                       PP.CTPRVN_CD = T.CTPRVN_CD
                   AND PP.BASE_YM = T.BASE_YM)
            , (SELECT
                   POPLTN_CO
               FROM
                   ctprvn_accto_popltn_info AS PP
               WHERE
                     PP.CTPRVN_CD = T.CTPRVN_CD
                 AND PP.BASE_YM = (SELECT
                                       MAX(BASE_YM) AS BASE_YM
                                   FROM
                                       ctprvn_accto_popltn_info AS P
                                   WHERE
                                       P.CTPRVN_CD = T.CTPRVN_CD))
          ))   as POP
     FROM
         colct_movie_mt_accto_ctprvn_accto_stats AS T
     WHERE
         T.BASE_YM = ?
     UNION ALL
     SELECT
         M.BASE_YM
       , M.BASE_YEAR
       , M.BASE_MT
       , '00' AS CTPRVN_CD
       , '전국' AS CTPRVN_NM
       , M.SCRNG_MOVIE_CO
       , M.MOVIE_ADNC_CO
       , M.EXPNDTR_PRICE
       , M.MOVIE_ADNC_CO * 1000 / (
         IFNULL(
                 (SELECT
                      POPLTN_CO
                  FROM
                      ctprvn_accto_popltn_info AS PP
                  WHERE
                        PP.CTPRVN_CD = '00'
                    AND PP.BASE_YM = M.BASE_YM)
             , (SELECT
                    POPLTN_CO
                FROM
                    ctprvn_accto_popltn_info AS PP
                WHERE
                      PP.CTPRVN_CD = '00'
                  AND PP.BASE_YM = (SELECT
                                        MAX(BASE_YM) AS BASE_YM
                                    FROM
                                        ctprvn_accto_popltn_info AS P
                                    WHERE
                                        P.CTPRVN_CD = '00'))
         )
         )    AS POPULATION_PER_ADNC_CO
       , (
             IFNULL(
                     (SELECT
                          POPLTN_CO
                      FROM
                          ctprvn_accto_popltn_info AS PP
                      WHERE
                            PP.CTPRVN_CD = '00'
                        AND PP.BASE_YM = M.BASE_YM)
                 , (SELECT
                        POPLTN_CO
                    FROM
                        ctprvn_accto_popltn_info AS PP
                    WHERE
                          PP.CTPRVN_CD = '00'
                      AND PP.BASE_YM = (SELECT
                                            MAX(BASE_YM) AS BASE_YM
                                        FROM
                                            ctprvn_accto_popltn_info AS P
                                        WHERE
                                            P.CTPRVN_CD = '00'))
             )
             )
     FROM
         colct_movie_mt_accto_sales_stats AS M
     WHERE
         M.BASE_YM = ?) AS DATA
        JOIN (SELECT
                  SUM(SCRNG_MOVIE_CO) / 12                                        AS STRD_SCRNG_MOVIE_CO
                , SUM(MOVIE_ADNC_CO) / 12                                         AS ADNC_CO_SUM
                , SUM(EXPNDTR_PRICE) / 12                                         AS EXPNDTR_PRICE_SUM
                , (SUM(MOVIE_ADNC_CO) / 12 * 1000) / (select
                                                          AVG(C.POPLTN_CO)
                                                      from
                                                          ctprvn_accto_popltn_info as C
                                                      where
                                                            C.CTPRVN_CD = '11'
                                                        and C.BASE_YEAR = '2022') as STD_POPLTN_PER_ADNC_CO
                , (SUM(EXPNDTR_PRICE) / 12) / (select
                                                   AVG(C.POPLTN_CO)
                                               from
                                                   ctprvn_accto_popltn_info as C
                                               where
                                                     C.CTPRVN_CD = '11'
                                                 and C.BASE_YEAR = '2022')        as STD_PRICE
              FROM
                  colct_movie_mt_accto_ctprvn_accto_stats
              WHERE
                    BASE_YEAR = '2022'
                AND CTPRVN_CD = '11') AS STD
             ON 1 = 1
ON DUPLICATE KEY UPDATE
                     SCRNG_MOVIE_CO           = VALUES(SCRNG_MOVIE_CO)
                   , MOVIE_ADNC_CO            = VALUES(MOVIE_ADNC_CO)
                   , EXPNDTR_PRICE            = VALUES(EXPNDTR_PRICE)
                   , POPLTN_PER_MOVIE_ADNC_CO = VALUES(POPLTN_PER_MOVIE_ADNC_CO)
                   , MOVIE_ADNC_CO_SCORE      = VALUES(MOVIE_ADNC_CO_SCORE)
                   , EXPNDTR_PRICE_SCORE      = VALUES(EXPNDTR_PRICE_SCORE)
                   , GNRLZ_SCORE              = VALUES(GNRLZ_SCORE)
                   , STDR_SCRNG_MOVIE_CO      = VALUES(STDR_SCRNG_MOVIE_CO)
                   , STDR_MOVIE_ADNC_CO       = VALUES(STDR_MOVIE_ADNC_CO)
                   , STDR_EXPNDTR_PRICE       = VALUES(STDR_EXPNDTR_PRICE)