insert into pblprfr_activate_crstat
(BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM,
 RASNG_CUTIN_CO, PBLPRFR_CO, VIEWING_NMPR_CO, EXPNDTR_PRICE,
 RASNG_CUTIN_RT, POPLTN_PER_VIEWING_NMPR_CO,
 VIEWING_NMPR_CO_SCORE, EXPNDTR_PRICE_SCORE, GNRLZ_SCORE,
 LRGE_THEAT_CO, MIDDL_THEAT_CO, SMALL_THEAT_CO,
 STDR_RASNG_CUTIN_CO, STDR_PBPRFR_CO, STDR_VIEWING_NMPR_CO,
 STDR_EXPNDTR_PRICE)
SELECT LOCAL.BASE_YM
     , SUBSTR(BASE_YM, 1, 4)             as BASE_YEAR
     , SUBSTR(BASE_YM, 5, 2)             as BASE_MT
     , LOCAL.CTPRVN_CD
     , LOCAL.CTPRVN_NM
     , LOCAL.PBLPRFR_RASNG_CUTIN_CO
     , LOCAL.PBLPRFR_CO
     , LOCAL.PBLPRFR_VIEWNG_NMPR_CO
     , LOCAL.PBLPRFR_SALES_PRICE
     , LOCAL.RASNG_CUTIN_RATE
     , LOCAL.POPLTN_PER_VIEWNG_NMPR_CO
     , LOCAL.POPLTN_PER_VIEWNG_NMPR_CO / STD.STD_VIEWNG *
       100                               AS VIEWNG_NMPR_CO_SCORE
     , LOCAL.PBLPRFR_SALES_PRICE / LOCAL.POP /
       STD.STD_PRICE *
       100                               AS EXPNDTR_PRICE_SCORE
     , (LOCAL.POPLTN_PER_VIEWNG_NMPR_CO / STD.STD_VIEWNG *
        100 + LOCAL.PBLPRFR_SALES_PRICE / LOCAL.POP /
              STD.STD_PRICE * 100) / 2   AS GNRLZ_SCORE
     , (select LRGE_THEAT_CO
        from pblprfr_fclty_crstat as F
        where F.CTPRVN_CD = LOCAL.CTPRVN_CD
          and F.BASE_YM = LOCAL.base_ym) AS LRGE_THEAT_CO
     , (select MIDDL_THEAT_CO
        from pblprfr_fclty_crstat as F
        where F.CTPRVN_CD = LOCAL.CTPRVN_CD
          and F.BASE_YM = LOCAL.base_ym) AS MIDDL_THEAT_CO
     , (select SMALL_THEAT_CO
        from pblprfr_fclty_crstat as F
        where F.CTPRVN_CD = LOCAL.CTPRVN_CD
          and F.BASE_YM = LOCAL.base_ym) AS SMALL_THEAT_CO
     , STD.RASNG_CUTIN_CO                AS STDR_RASNG_CUTIN_CO
     , STD.PBLPRFR_CO                    AS STDR_PBPRFR_CO
     , STD.VIEWNG_NMPR_CO                AS STDR_VIEWING_NMPR_CO
     , STD.EXPNDTR_PRICE                 AS STDR_EXPNDTR_PRICE
FROM (SELECT BASE_YM
           , CTPRVN_CD
           , MAX(CTPRVN_NM)                  as CTPRVN_NM
           , SUM(PBLPRFR_RASNG_CUTIN_CO)     as PBLPRFR_RASNG_CUTIN_CO
           , SUM(PBLPRFR_CO)                 as PBLPRFR_CO
           , SUM(PBLPRFR_VIEWNG_NMPR_CO)     as PBLPRFR_VIEWNG_NMPR_CO
           , SUM(PBLPRFR_SALES_PRICE) * 1000 as PBLPRFR_SALES_PRICE
           , (CASE SUM(PBLPRFR_CO)
                  WHEN 0 THEN 0
                  ELSE IFNULL(SUM(PBLPRFR_RASNG_CUTIN_CO) /
                              SUM(PBLPRFR_CO) * 100, 0)
        END)                                 AS RASNG_CUTIN_RATE
           , SUM(PBLPRFR_VIEWNG_NMPR_CO) * 1000 / (
        IFNULL(
                (SELECT POPLTN_CO
                 FROM ctprvn_accto_popltn_info AS PP
                 WHERE PP.CTPRVN_CD = T.CTPRVN_CD
                   AND PP.BASE_YM = T.BASE_YM)
            , (SELECT POPLTN_CO
               FROM ctprvn_accto_popltn_info AS PP
               WHERE PP.CTPRVN_CD = T.CTPRVN_CD
                 AND PP.BASE_YM =
                     (SELECT MAX(BASE_YM) AS BASE_YM
                      FROM ctprvn_accto_popltn_info AS P
                      WHERE P.CTPRVN_CD = T.CTPRVN_CD))
        )
        )                                    AS POPLTN_PER_VIEWNG_NMPR_CO
           , IFNULL(
            (SELECT POPLTN_CO
             FROM ctprvn_accto_popltn_info AS PP
             WHERE PP.CTPRVN_CD = T.CTPRVN_CD
               AND PP.BASE_YM = T.BASE_YM)
        , (SELECT POPLTN_CO
           FROM ctprvn_accto_popltn_info AS PP
           WHERE PP.CTPRVN_CD = T.CTPRVN_CD
             AND PP.BASE_YM =
                 (SELECT MAX(BASE_YM) AS BASE_YM
                  FROM ctprvn_accto_popltn_info AS P
                  WHERE P.CTPRVN_CD = T.CTPRVN_CD))
             )                               as POP
      FROM colct_pblprfr_viewng_mt_accto_ctprvn_accto_stats AS T
      WHERE BASE_YM = ?
      group by BASE_YM, CTPRVN_CD
      union all
      SELECT BASE_YM
           , '00'                            as CTPRVN_CD
           , '전국'                            as CTPRVN_NM
           , SUM(PBLPRFR_RASNG_CUTIN_CO)     as PBLPRFR_RASNG_CUTIN_CO
           , SUM(PBLPRFR_CO)                 as PBLPRFR_CO
           , SUM(PBLPRFR_VIEWNG_NMPR_CO)     as PBLPRFR_VIEWNG_NMPR_CO
           , SUM(PBLPRFR_SALES_PRICE) * 1000 as PBLPRFR_SALES_PRICE
           , (CASE SUM(PBLPRFR_CO)
                  WHEN 0 THEN 0
                  ELSE IFNULL(SUM(PBLPRFR_RASNG_CUTIN_CO) /
                              SUM(PBLPRFR_CO) * 100, 0)
          END)                               AS RASNG_CUTIN_RATE
           , SUM(PBLPRFR_VIEWNG_NMPR_CO) * 1000 / (
          IFNULL(
                  (SELECT POPLTN_CO
                   FROM ctprvn_accto_popltn_info AS PP
                   WHERE PP.CTPRVN_CD = '00'
                     AND PP.BASE_YM = T.BASE_YM)
              , (SELECT POPLTN_CO
                 FROM ctprvn_accto_popltn_info AS PP
                 WHERE PP.CTPRVN_CD = '00'
                   AND PP.BASE_YM =
                       (SELECT MAX(BASE_YM) AS BASE_YM
                        FROM ctprvn_accto_popltn_info AS P
                        WHERE P.CTPRVN_CD = '00'))
          )
          )                                  AS POPLTN_PER_VIEWNG_NMPR_CO
           , IFNULL(
              (SELECT POPLTN_CO
               FROM ctprvn_accto_popltn_info AS PP
               WHERE PP.CTPRVN_CD = '00'
                 AND PP.BASE_YM = T.BASE_YM)
          , (SELECT POPLTN_CO
             FROM ctprvn_accto_popltn_info AS PP
             WHERE PP.CTPRVN_CD = '00'
               AND PP.BASE_YM =
                   (SELECT MAX(BASE_YM) AS BASE_YM
                    FROM ctprvn_accto_popltn_info AS P
                    WHERE P.CTPRVN_CD = '00'))
             )                               as POP
      FROM colct_pblprfr_viewng_mt_accto_ctprvn_accto_stats AS T
      WHERE BASE_YM = ?
      group by BASE_YM) AS LOCAL
         JOIN (SELECT BASE_YEAR
                    , SUM(PBLPRFR_RASNG_CUTIN_CO) / 12     AS RASNG_CUTIN_CO
                    , SUM(PBLPRFR_CO) / 12                 AS PBLPRFR_CO
                    , SUM(PBLPRFR_VIEWNG_NMPR_CO) / 12     AS VIEWNG_NMPR_CO
                    , SUM(PBLPRFR_SALES_PRICE) * 1000 / 12 AS EXPNDTR_PRICE
                    , (SUM(PBLPRFR_VIEWNG_NMPR_CO) / 12 *
                       1000) / (select C.POPLTN_CO
                                from ctprvn_accto_popltn_info as C
                                where C.CTPRVN_CD = '11'
                                  and C.BASE_YEAR = '2022'
                                  and C.BASE_MT = '12')    as STD_VIEWNG
                    , (SUM(PBLPRFR_SALES_PRICE) * 1000 / 12) /
                      (select C.POPLTN_CO
                       from ctprvn_accto_popltn_info as C
                       where C.CTPRVN_CD = '11'
                         and C.BASE_YEAR = '2022'
                         and C.BASE_MT = '12')             as STD_PRICE
               FROM colct_pblprfr_viewng_mt_accto_ctprvn_accto_stats
               WHERE BASE_YEAR = '2022'
                 and CTPRVN_CD = '11') AS STD;