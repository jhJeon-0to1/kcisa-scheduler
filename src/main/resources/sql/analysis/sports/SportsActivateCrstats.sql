INSERT INTO sports_activate_crstat
(BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM,
 SPORTS_MATCH_CO, SPORTS_VIEWNG_NMPR_CO,
 POPLTN_PER_VIEWNG_NMPR_CO, SPORTS_VIEWNG_NMPR_CO_SCORE,
 GNRLZ_SCORE, STDR_SPORTS_MATCH_CO,
 STDR_SPORTS_VIEWNG_NMPR_CO)
SELECT DATA.BASE_DE AS BASE_YM
     , DATA.BASE_YEAR
     , DATA.BASE_MONTH
     , DATA.CTPRVN_CD
     , DATA.CTPRVN_NM
     , DATA.SPORTS_MATCH_CO
     , DATA.SPORTS_VIEWNG_NMPR_CO
     , DATA.POPLTN_PER_VIEWNG_NMPR_CO
     , (DATA.POPLTN_PER_VIEWNG_NMPR_CO) / STD.STD_VALUE *
       100          as SPORTS_VIEWNG_NMPR_CO_SCORE
     , (DATA.POPLTN_PER_VIEWNG_NMPR_CO) / STD.STD_VALUE *
       100          as GNRLZ_SCORE
     , STD.MATCH_CO
     , STD.VIEWNG_NMPR_CO
FROM (SELECT CONCAT(BASE_YEAR, BASE_MT)        AS BASE_DE
           , BASE_YEAR                         as BASE_YEAR
           , BASE_MT                           as BASE_MONTH
           , CTPRVN_CD                         as CTPRVN_CD
           , (SELECT CTPRVN_NM
              FROM ctprvn_info AS C
              WHERE C.CTPRVN_CD = S.CTPRVN_CD) AS CTPRVN_NM
           , SUM(SPORTS_MATCH_CO)              AS SPORTS_MATCH_CO
           , SUM(SPORTS_VIEWNG_NMPR_CO)        AS SPORTS_VIEWNG_NMPR_CO
           , SUM(SPORTS_VIEWNG_NMPR_CO) * 1000 / (
        IFNULL(
                (SELECT POPLTN_CO
                 FROM ctprvn_accto_popltn_info AS PP
                 WHERE PP.CTPRVN_CD = S.CTPRVN_CD
                   AND PP.BASE_YM =
                       CONCAT(S.BASE_YEAR, S.BASE_MT))
            , (SELECT POPLTN_CO
               FROM ctprvn_accto_popltn_info AS PP
               WHERE PP.CTPRVN_CD = S.CTPRVN_CD
                 AND PP.BASE_YM =
                     (SELECT MAX(BASE_YM) AS BASE_YM
                      FROM ctprvn_accto_popltn_info AS P
                      WHERE P.CTPRVN_CD = S.CTPRVN_CD))
        )
        )                                      AS POPLTN_PER_VIEWNG_NMPR_CO
      FROM colct_sports_viewng_info AS S
      where BASE_YEAR = ?
        and BASE_MT = ?
      GROUP BY BASE_YEAR, BASE_MT, CTPRVN_CD
      UNION ALL
      SELECT CONCAT(BASE_YEAR, BASE_MT) AS BASE_DE
           , BASE_YEAR                  as BASE_YEAR
           , BASE_MT                    as BASE_MONTH
           , '00'                       as CTPRVN_CD
           , '전국'                       AS CTPRVN_NM
           , SUM(SPORTS_MATCH_CO)       AS SPORTS_MATCH_CO
           , SUM(SPORTS_VIEWNG_NMPR_CO) AS SPORTS_VIEWNG_NMPR_CO
           , SUM(SPORTS_VIEWNG_NMPR_CO) * 1000 / (
          IFNULL(
                  (SELECT POPLTN_CO
                   FROM ctprvn_accto_popltn_info AS PP
                   WHERE PP.CTPRVN_CD = '00'
                     AND PP.BASE_YM =
                         CONCAT(S.BASE_YEAR, S.BASE_MT))
              , (SELECT POPLTN_CO
                 FROM ctprvn_accto_popltn_info AS PP
                 WHERE PP.CTPRVN_CD = '00'
                   AND PP.BASE_YM =
                       (SELECT MAX(BASE_YM) AS BASE_YM
                        FROM ctprvn_accto_popltn_info AS P
                        WHERE P.CTPRVN_CD = '00'))
          )
          )                             AS POPLTN_PER_VIEWNG_NMPR_CO
      FROM colct_sports_viewng_info AS S
      where BASE_YEAR = ?
        and BASE_MT = ?
      GROUP BY BASE_YEAR, BASE_MT
      ORDER BY 1, 4) AS DATA
         JOIN (SELECT SUM(SPORTS_MATCH_CO) / 12       AS MATCH_CO
                    , SUM(SPORTS_VIEWNG_NMPR_CO) / 12 AS VIEWNG_NMPR_CO
                    , (
            (SUM(SPORTS_VIEWNG_NMPR_CO) / 12 * 1000) /
            (select C.POPLTN_CO
             from ctprvn_accto_popltn_info as C
             where C.CTPRVN_CD = '11'
               and C.BASE_YEAR = '2022'
               and C.BASE_MT = '12'))                 as STD_VALUE
               FROM colct_sports_viewng_info AS S
               WHERE BASE_YEAR = '2022'
                 AND CTPRVN_CD = '11') AS STD
