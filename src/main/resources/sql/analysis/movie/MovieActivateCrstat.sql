INSERT INTO
    analysis_model.movie_activate_crstat
(BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM,
 SCRNG_MOVIE_CO, MOVIE_ADNC_CO, EXPNDTR_PRICE,
 POPLTN_PER_MOVIE_ADNC_CO, MOVIE_ADNC_CO_SCORE,
 EXPNDTR_PRICE_SCORE, GNRLZ_SCORE, STDR_SCRNG_MOVIE_CO,
 STDR_MOVIE_ADNC_CO, STDR_EXPNDTR_PRICE)
SELECT
    DATA.BASE_YM
  , SUBSTR(DATA.BASE_YM, 1, 4) AS BASE_YEAR
  , SUBSTR(DATA.BASE_YM, 5, 2) AS BASE_MT
  , DATA.CTPRVN_CD
  , DATA.CTPRVN_NM
  , DATA.SCRNG_MOVIE_CO
  , DATA.MOVIE_ADNC_CO
  , DATA.EXPNDTR_PRICE
  , DATA.POPLTN_PER_ADNC_CO
  , (IF(STD.POPLTN_PER_ADNC_CO = 0, null,
        DATA.POPLTN_PER_ADNC_CO / STD.POPLTN_PER_ADNC_CO *
        100))                  AS MOVIE_ADNC_CO_SCORE
  , (IF(STD.POPLTN_PER_EXPNDTR_PRICE = 0, null,
        DATA.POPLTN_PER_EXPNDTR_PRICE /
        STD.POPLTN_PER_EXPNDTR_PRICE *
        100))                  AS EXPNDTR_PRICE_SCORE
  , (IF(STD.POPLTN_PER_ADNC_CO = 0, null,
        DATA.POPLTN_PER_ADNC_CO / STD.POPLTN_PER_ADNC_CO *
        100) +
     IF(STD.POPLTN_PER_EXPNDTR_PRICE = 0, null,
        DATA.POPLTN_PER_EXPNDTR_PRICE /
        STD.POPLTN_PER_EXPNDTR_PRICE *
        100)) /
    2                          AS GNRLZ_SCORE
  , STD.SCRNG_MOVIE_CO         AS STDR_SCRNG_MOVIE_CO
  , STD.MOVIE_ADNC_CO          AS STDR_MOVIE_ADNC_CO
  , STD.EXPNDTR_PRICE          AS STDR_EXPNDTR_PRICE
FROM (SELECT
          LOCAL.BASE_YM
        , LOCAL.CTPRVN_CD
        , LOCAL.CTPRVN_NM
        , LOCAL.SCRNG_MOVIE_CO
        , LOCAL.MOVIE_ADNC_CO
        , LOCAL.EXPNDTR_PRICE
        , LOCAL.MOVIE_ADNC_CO * 1000 /
          POP.POPLTN_CO AS POPLTN_PER_ADNC_CO
        , LOCAL.EXPNDTR_PRICE / POP.POPLTN_CO *
          1000          AS POPLTN_PER_EXPNDTR_PRICE
      FROM (SELECT
                BASE_YM
              , CTPRVN_CD
              , CTPRVN_NM
              , SCRNG_MOVIE_CO
              , MOVIE_ADNC_CO
              , EXPNDTR_PRICE
            FROM colct_movie_mt_accto_ctprvn_accto_stats
            WHERE
                BASE_YM = ?)        AS LOCAL
      JOIN ctprvn_accto_popltn_info AS POP
           ON POP.BASE_YM = LOCAL.BASE_YM AND
              POP.CTPRVN_CD = LOCAL.CTPRVN_CD
      UNION ALL
      SELECT
          NATION.BASE_YM
        , NATION.CTPRVN_CD
        , NATION.CTPRVN_NM
        , NATION.SCRNG_MOVIE_CO
        , NATION.MOVIE_ADNC_CO
        , NATION.EXPNDTR_PRICE
        , NATION.MOVIE_ADNC_CO * 1000 /
          POP.POPLTN_CO AS POPLTN_PER_ADNC_CO
        , NATION.EXPNDTR_PRICE / POP.POPLTN_CO *
          1000          AS POPLTN_PER_EXPNDTR_PRICE
      FROM (SELECT
                BASE_YM
              , '00' AS CTPRVN_CD
              , '전국' AS CTPRVN_NM
              , SCRNG_MOVIE_CO
              , MOVIE_ADNC_CO
              , EXPNDTR_PRICE
            FROM colct_movie_mt_accto_sales_stats
            WHERE
                BASE_YM = ?)        AS NATION
      JOIN ctprvn_accto_popltn_info AS POP
           ON NATION.BASE_YM = POP.BASE_YM AND
              POP.CTPRVN_CD = '00')               AS DATA
JOIN (SELECT
          SEOUL.SCRNG_MOVIE_CO
        , SEOUL.MOVIE_ADNC_CO
        , SEOUL.EXPNDTR_PRICE
        , SEOUL.MOVIE_ADNC_CO * 1000 /
          SEOUL_POP.POPLTN_CO AS POPLTN_PER_ADNC_CO
        , SEOUL.EXPNDTR_PRICE / SEOUL_POP.POPLTN_CO *
          1000                AS POPLTN_PER_EXPNDTR_PRICE
      FROM (SELECT
                SUM(SCRNG_MOVIE_CO) / 12 AS SCRNG_MOVIE_CO
              , SUM(MOVIE_ADNC_CO) / 12  AS MOVIE_ADNC_CO
              , SUM(EXPNDTR_PRICE) / 12  AS EXPNDTR_PRICE
            FROM colct_movie_mt_accto_ctprvn_accto_stats
            WHERE
                  BASE_YEAR = '2022'
              AND CTPRVN_CD = '11'
            GROUP BY CTPRVN_CD)     AS SEOUL
      JOIN (SELECT
                AVG(POPLTN_CO) AS POPLTN_CO
            FROM ctprvn_accto_popltn_info
            WHERE
                  BASE_YEAR = '2022'
              AND CTPRVN_CD = '11') AS SEOUL_POP) AS STD
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
;
