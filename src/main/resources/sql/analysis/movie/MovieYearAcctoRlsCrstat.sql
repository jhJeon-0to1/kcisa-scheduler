INSERT INTO
    analysis_model.movie_year_accto_rls_crstat
(BASE_YEAR, CTPRVN_CD, CTPRVN_NM,
 SCRNG_MOVIE_CO, RLS_MOVIE_CO, RLS_MOVIE_RT,
 DRAMA_RLS_MOVIE_CO, HORROR_RLS_MOVIE_CO,
 CRIME_RLS_MOVIE_CO, COMEDY_RLS_MOVIE_CO, ANM_RLS_MOVIE_CO,
 ACTN_RLS_MOVIE_CO, ROMC_RLS_MOVIE_CO, DCMTY_RLS_MOVIE_CO,
 THLR_RLS_MOVIE_CO, FAM_RLS_MOVIE_CO, ADVT_RLS_MOVIE_CO,
 FANTY_RLS_MOVIE_CO, MYSTY_RLS_MOVIE_CO,
 PBLPRFR_RLS_MOVIE_CO, SF_RLS_MOVIE_CO, MUSICL_RLS_MOVIE_CO,
 HISTY_RLS_MOVIE_CO, WAR_RLS_MOVIE_CO, WT_RLS_MOVIE_CO,
 ADULT_RLS_MOVIE_CO, ETC_RLS_MOVIE_CO, METRP_AREA_AT)
SELECT
    (DATA.BASE_YEAR)
  , DATA.CTPRVN_CD
  , (DATA.CTPRVN_NM)
  , (DATA.SCRNG_MOVIE_CO)
  , (DATA.RLS_MOVIE_CO)
  , (DATA.RLS_MOVIE_RT)
  , IFNULL(GENRE.DRAMA_RLS_MOVIE_CO,
           0)                         AS DRAMA_RLS_MOVIE_CO
  , IFNULL(GENRE.HORROR_RLS_MOVIE_CO,
           0)                         AS HORROR_RLS_MOVIE_CO
  , IFNULL(GENRE.CRIME_RLS_MOVIE_CO,
           0)                         AS CRIME_RLS_MOVIE_CO
  , IFNULL(GENRE.COMEDY_RLS_MOVIE_CO,
           0)                         AS COMEDY_RLS_MOVIE_CO
  , IFNULL(GENRE.ANM_RLS_MOVIE_CO, 0) AS ANM_RLS_MOVIE_CO
  , IFNULL(GENRE.ACTN_RLS_MOVIE_CO,
           0)                         AS ACTN_RLS_MOVIE_CO
  , IFNULL(GENRE.ROMC_RLS_MOVIE_CO,
           0)                         AS ROMC_RLS_MOVIE_CO
  , IFNULL(GENRE.DCMTY_RLS_MOVIE_CO,
           0)                         AS DCMTY_RLS_MOVIE_CO
  , IFNULL(GENRE.THLR_RLS_MOVIE_CO,
           0)                         AS THLR_RLS_MOVIE_CO
  , IFNULL(GENRE.FAM_RLS_MOVIE_CO, 0) AS FAM_RLS_MOVIE_CO
  , IFNULL(GENRE.ADVT_RLS_MOVIE_CO,
           0)                         AS ADVT_RLS_MOVIE_CO
  , IFNULL(GENRE.FANTY_RLS_MOVIE_CO,
           0)                         AS FANTY_RLS_MOVIE_CO
  , IFNULL(GENRE.MYSTY_RLS_MOVIE_CO,
           0)                         AS MYSTY_RLS_MOVIE_CO
  , IFNULL(GENRE.PBLPRFR_RLS_MOVIE_CO,
           0)                         AS PBLPRFR_RLS_MOVIE_CO
  , IFNULL(GENRE.SF_RLS_MOVIE_CO, 0)  AS SF_RLS_MOVIE_CO
  , IFNULL(GENRE.MUSICL_RLS_MOVIE_CO,
           0)                         AS MUSICL_RLS_MOVIE_CO
  , IFNULL(GENRE.HISTY_RLS_MOVIE_CO,
           0)                         AS HISTY_RLS_MOVIE_CO
  , IFNULL(GENRE.WAR_RLS_MOVIE_CO, 0) AS WAR_RLS_MOVIE_CO
  , IFNULL(GENRE.WT_RLS_MOVIE_CO, 0)  AS WT_RLS_MOVIE_CO
  , IFNULL(GENRE.ADULT_RLS_MOVIE_CO,
           0)                         AS ADULT_RLS_MOVIE_CO
  , IFNULL(GENRE.ETC_RLS_MOVIE_CO, 0) AS ETC_RLS_MOVIE_CO
  , (DATA.METRP_AREA_AT)
FROM ((SELECT
           T.BASE_YEAR                     AS BASE_YEAR
         , T.CTPRVN_CD                     AS CTPRVN_CD
         , T.CTPRVN_NM                     AS CTPRVN_NM
         , T.SCRNG_MOVIE_CO                AS SCRNG_MOVIE_CO
         , M.RLS_MOVIE_CO                  AS RLS_MOVIE_CO
         , M.RLS_MOVIE_CO / T.SCRNG_MOVIE_CO *
           100                             AS RLS_MOVIE_RT
         , (SELECT METRP_AT
            FROM ctprvn_info AS P
            WHERE
                T.CTPRVN_CD = P.CTPRVN_CD) AS METRP_AREA_AT
       FROM colct_movie_year_accto_ctprvn_accto_stats AS T
       JOIN colct_movie_year_accto_sales_stats        AS M
            ON T.BASE_YEAR = M.BASE_YEAR
       WHERE
           M.BASE_YEAR = ?)
      UNION ALL
      (SELECT
           BASE_YEAR                           AS BASE_YEAR
         , '00'                                AS CTPRVN_CD
         , '전국'                                AS CTPRVN_NM
         , SCRNG_MOVIE_CO                      AS SCRNG_MOVIE_CO
         , RLS_MOVIE_CO                        AS RLS_MOVIE_CO
         , RLS_MOVIE_CO / SCRNG_MOVIE_CO * 100 AS RLS_MOVIE_RT
         , 'N'                                 AS METRP_AREA_AT
       FROM colct_movie_year_accto_sales_stats AS M
       where
           M.BASE_YEAR = ?))   AS DATA
LEFT JOIN (SELECT
               BASE_YEAR
             , SUM(IF(REPRSNT_GENRE_NM = '드라마',
                      MOVIE_CO,
                      0)) AS DRAMA_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '공포(호러)',
                      MOVIE_CO,
                      0)) AS HORROR_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '범죄',
                      MOVIE_CO,
                      0)) AS CRIME_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '코미디',
                      MOVIE_CO,
                      0)) AS COMEDY_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '애니메이션',
                      MOVIE_CO,
                      0)) AS ANM_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '액션',
                      MOVIE_CO,
                      0)) AS ACTN_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '멜로/로맨스',
                      MOVIE_CO,
                      0)) AS ROMC_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '다큐멘터리',
                      MOVIE_CO,
                      0)) AS DCMTY_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '스릴러',
                      MOVIE_CO,
                      0)) AS THLR_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '가족',
                      MOVIE_CO,
                      0)) AS FAM_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '어드벤처',
                      MOVIE_CO,
                      0)) AS ADVT_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '판타지',
                      MOVIE_CO,
                      0)) AS FANTY_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '미스터리',
                      MOVIE_CO,
                      0)) AS MYSTY_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '공연',
                      MOVIE_CO,
                      0)) AS PBLPRFR_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = 'SF',
                      MOVIE_CO,
                      0)) AS SF_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '뮤지컬',
                      MOVIE_CO,
                      0)) AS MUSICL_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '사극',
                      MOVIE_CO,
                      0)) AS HISTY_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '기타',
                      MOVIE_CO,
                      0)) AS ETC_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '성인물(에로)',
                      MOVIE_CO,
                      0)) AS ADULT_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '전쟁',
                      MOVIE_CO,
                      0)) AS WAR_RLS_MOVIE_CO
             , SUM(IF(REPRSNT_GENRE_NM = '서부극(웨스턴)',
                      MOVIE_CO,
                      0)) AS WT_RLS_MOVIE_CO
           FROM (SELECT
                     SUBSTR(RLS_YEAR, 1, 4) AS BASE_YEAR
                   , REPRSNT_GENRE_NM
                   , COUNT(*)               AS MOVIE_CO
                 FROM colct_movie_info
                 WHERE
                       REPRSNT_GENRE_NM is not null
                   and REPRSNT_GENRE_NM != ''
                   and SUBSTR(RLS_YEAR, 1, 4) = ?
                 GROUP BY
                     SUBSTR(RLS_YEAR, 1, 4),
                     REPRSNT_GENRE_NM) AS MOVIE
           GROUP BY BASE_YEAR) AS GENRE
          ON DATA.BASE_YEAR = GENRE.BASE_YEAR
ON DUPLICATE KEY UPDATE
                     SCRNG_MOVIE_CO       = VALUES(SCRNG_MOVIE_CO)
                   , RLS_MOVIE_CO         = VALUES(RLS_MOVIE_CO)
                   , RLS_MOVIE_RT         = VALUES(RLS_MOVIE_RT)
                   , DRAMA_RLS_MOVIE_CO   = VALUES(DRAMA_RLS_MOVIE_CO)
                   , HORROR_RLS_MOVIE_CO  = VALUES(HORROR_RLS_MOVIE_CO)
                   , CRIME_RLS_MOVIE_CO   = VALUES(CRIME_RLS_MOVIE_CO)
                   , COMEDY_RLS_MOVIE_CO  = VALUES(COMEDY_RLS_MOVIE_CO)
                   , ANM_RLS_MOVIE_CO     = VALUES(ANM_RLS_MOVIE_CO)
                   , ACTN_RLS_MOVIE_CO    = VALUES(ACTN_RLS_MOVIE_CO)
                   , ROMC_RLS_MOVIE_CO    = VALUES(ROMC_RLS_MOVIE_CO)
                   , DCMTY_RLS_MOVIE_CO   = VALUES(DCMTY_RLS_MOVIE_CO)
                   , THLR_RLS_MOVIE_CO    = VALUES(THLR_RLS_MOVIE_CO)
                   , FAM_RLS_MOVIE_CO     = VALUES(FAM_RLS_MOVIE_CO)
                   , ADVT_RLS_MOVIE_CO    = VALUES(ADVT_RLS_MOVIE_CO)
                   , FANTY_RLS_MOVIE_CO   = VALUES(FANTY_RLS_MOVIE_CO)
                   , MYSTY_RLS_MOVIE_CO   = VALUES(MYSTY_RLS_MOVIE_CO)
                   , PBLPRFR_RLS_MOVIE_CO = VALUES(PBLPRFR_RLS_MOVIE_CO)
                   , SF_RLS_MOVIE_CO      = VALUES(SF_RLS_MOVIE_CO)
                   , MUSICL_RLS_MOVIE_CO  = VALUES(MUSICL_RLS_MOVIE_CO)
                   , HISTY_RLS_MOVIE_CO   = VALUES(HISTY_RLS_MOVIE_CO)
                   , WAR_RLS_MOVIE_CO     = VALUES(WAR_RLS_MOVIE_CO)
                   , WT_RLS_MOVIE_CO      = VALUES(WT_RLS_MOVIE_CO)
                   , ADULT_RLS_MOVIE_CO   = VALUES(ADULT_RLS_MOVIE_CO)
                   , ETC_RLS_MOVIE_CO     = VALUES(ETC_RLS_MOVIE_CO)
                   , METRP_AREA_AT        = VALUES(METRP_AREA_AT);