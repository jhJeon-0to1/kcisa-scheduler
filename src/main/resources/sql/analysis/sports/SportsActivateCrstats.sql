INSERT INTO
    analysis_model.sports_activate_crstat
(BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM,
 SPORTS_MATCH_CO, SPORTS_VIEWNG_NMPR_CO,
 POPLTN_PER_VIEWNG_NMPR_CO, SPORTS_VIEWNG_NMPR_CO_SCORE,
 GNRLZ_SCORE, STDR_SPORTS_MATCH_CO,
 STDR_SPORTS_VIEWNG_NMPR_CO)
SELECT
    DATA.BASE_YM
  , SUBSTR(DATA.BASE_YM, 1, 4) AS BASE_YEAR
  , SUBSTR(DATA.BASE_YM, 5, 2) AS BASE_MONTH
  , DATA.CTPRVN_CD
  , DATA.CTPRVN_NM
  , DATA.SPORTS_MATCH_CO
  , DATA.SPORTS_VIEWNG_NMPR_CO
  , DATA.POPLTN_PER_VIEWNG_NMPR_CO
  , (IF(STD.POPLTN_PER_VIEWNG_NMPR_CO = 0, null,
        DATA.POPLTN_PER_VIEWNG_NMPR_CO /
        STD.POPLTN_PER_VIEWNG_NMPR_CO *
        100))                  AS SPORTS_VIEWNG_NMPR_CO_SCORE
  , (IF(STD.POPLTN_PER_VIEWNG_NMPR_CO = 0, null,
        DATA.POPLTN_PER_VIEWNG_NMPR_CO /
        STD.POPLTN_PER_VIEWNG_NMPR_CO *
        100))                  AS GNRLZ_SCORE
  , STD.SEOUL_MATCH_CO
  , STD.SEOUL_VIEWNG_NMPR_CO
FROM (SELECT
          LOCAL.BASE_YM
        , LOCAL.CTPRVN_CD
        , LOCAL.CTPRVN_NM
        , LOCAL.SPORTS_MATCH_CO
        , LOCAL.SPORTS_VIEWNG_NMPR_CO
        , LOCAL.SPORTS_VIEWNG_NMPR_CO * 1000 /
          POP.POPLTN_CO AS POPLTN_PER_VIEWNG_NMPR_CO
      FROM (SELECT
                SUBSTR(MAX(BASE_DE), 1, 6) AS BASE_YM
              , CTPRVN_CD
              , MAX(CTPRVN_NM)             AS CTPRVN_NM
              , SUM(SPORTS_MATCH_CO)       AS SPORTS_MATCH_CO
              , SUM(SPORTS_VIEWNG_NMPR_CO) AS SPORTS_VIEWNG_NMPR_CO
            FROM colct_sports_viewng_info
            WHERE
                BASE_DE BETWEEN ? AND ?
            GROUP BY CTPRVN_CD)     AS LOCAL
      JOIN ctprvn_accto_popltn_info AS POP
           ON POP.CTPRVN_CD = LOCAL.CTPRVN_CD
               AND POP.BASE_YM = LOCAL.BASE_YM
      UNION ALL
      SELECT
          NATION.BASE_YM
        , NATION.CTPRVN_CD
        , NATION.CTPRVN_NM
        , NATION.SPORTS_MATCH_CO
        , NATION.SPORTS_VIEWNG_NMPR_CO
        , NATION.SPORTS_VIEWNG_NMPR_CO * 1000 /
          POP.POPLTN_CO AS POPLTN_PER_VIEWNG_NMPR_CO
      FROM (SELECT
                SUBSTR(MAX(BASE_DE), 1, 6) AS BASE_YM
              , '00'                       AS CTPRVN_CD
              , '전국'                       AS CTPRVN_NM
              , SUM(SPORTS_MATCH_CO)       AS SPORTS_MATCH_CO
              , SUM(SPORTS_VIEWNG_NMPR_CO) AS SPORTS_VIEWNG_NMPR_CO
            FROM colct_sports_viewng_info
            WHERE
                BASE_DE BETWEEN ? AND ?) AS NATION
      JOIN ctprvn_accto_popltn_info      AS POP
           ON POP.CTPRVN_CD = NATION.CTPRVN_CD
               AND POP.BASE_YM = NATION.BASE_YM)    AS DATA
JOIN (SELECT
          SEOUL.MATCH_CO       AS SEOUL_MATCH_CO
        , SEOUL.VIEWNG_NMPR_CO AS SEOUL_VIEWNG_NMPR_CO
        , SEOUL.VIEWNG_NMPR_CO * 1000 /
          SEOUL_POP.POPLTN_CO  AS POPLTN_PER_VIEWNG_NMPR_CO
      FROM (SELECT
                SUM(SPORTS_MATCH_CO) / 12       AS MATCH_CO
              , SUM(SPORTS_VIEWNG_NMPR_CO) / 12 AS VIEWNG_NMPR_CO
            FROM colct_sports_viewng_info
            WHERE
                  BASE_YEAR = '2022'
              AND CTPRVN_CD = '11'
            GROUP BY
                CTPRVN_CD)            AS SEOUL
      JOIN (SELECT
                AVG(POPLTN_CO) AS POPLTN_CO
            FROM ctprvn_accto_popltn_info
            WHERE
                  CTPRVN_CD = '11'
              AND BASE_YEAR = '2022') AS SEOUL_POP) AS STD
     ON 1 = 1
ON DUPLICATE KEY UPDATE
                     BASE_YM                     = VALUES(BASE_YM)
                   , BASE_YEAR                   = VALUES(BASE_YEAR)
                   , BASE_MONTH                  = VALUES(BASE_MONTH)
                   , CTPRVN_CD                   = VALUES(CTPRVN_CD)
                   , CTPRVN_NM                   = VALUES(CTPRVN_NM)
                   , SPORTS_MATCH_CO             = VALUES(SPORTS_MATCH_CO)
                   , SPORTS_VIEWNG_NMPR_CO       = VALUES(SPORTS_VIEWNG_NMPR_CO)
                   , POPLTN_PER_VIEWNG_NMPR_CO   = VALUES(POPLTN_PER_VIEWNG_NMPR_CO)
                   , SPORTS_VIEWNG_NMPR_CO_SCORE = VALUES(SPORTS_VIEWNG_NMPR_CO_SCORE)
                   , GNRLZ_SCORE                 = VALUES(GNRLZ_SCORE)
                   , STDR_SPORTS_MATCH_CO        = VALUES(STDR_SPORTS_MATCH_CO)
                   , STDR_SPORTS_VIEWNG_NMPR_CO  = VALUES(STDR_SPORTS_VIEWNG_NMPR_CO)
;