insert into movie_rls_crstat
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD,
 CTPRVN_NM, SCRNG_MOVIE_CO, RLS_MOVIE_CO, RLS_MOVIE_RT,
 DRAMA_RLS_MOVIE_CO, HORROR_RLS_MOVIE_CO,
 CRIME_RLS_MOVIE_CO, COMEDY_RLS_MOVIE_CO, ANM_RLS_MOVIE_CO,
 ACTN_RLS_MOVIE_CO, ROMC_RLS_MOVIE_CO, DCMTY_RLS_MOVIE_CO,
 THLR_RLS_MOVIE_CO, FAM_RLS_MOVIE_CO, ADVT_RLS_MOVIE_CO,
 FANTY_RLS_MOVIE_CO, MYSTY_RLS_MOVIE_CO,
 PBLPRFR_RLS_MOVIE_CO, SF_RLS_MOVIE_CO, MUSICL_RLS_MOVIE_CO,
 HISTY_RLS_MOVIE_CO, WAR_RLS_MOVIE_CO, WT_RLS_MOVIE_CO,
 ADULT_RLS_MOVIE_CO, ETC_RLS_MOVIE_CO, METRP_AREA_AT)
SELECT DATA.BASE_DE
     , SUBSTR(DATA.BASE_DE, 1, 4) AS BASE_YEAR
     , SUBSTR(DATA.BASE_DE, 5, 2) AS BASE_MT
     , SUBSTR(DATA.BASE_DE, 7, 2) AS BASE_DAY
     , DATA.CTPRVN_CD
     , (DATA.CTPRVN_NM)
     , (DATA.SCRNG_MOVIE_CO)
     , (DATA.RLS_MOVIE_CO)
     , (DATA.RLS_MOVIE_RT)
     , (GENRE.DRAMA_RLS_MOVIE_CO)
     , (GENRE.HORROR_RLS_MOVIE_CO)
     , (GENRE.CRIME_RLS_MOVIE_CO)
     , (GENRE.COMEDY_RLS_MOVIE_CO)
     , (GENRE.ANM_RLS_MOVIE_CO)
     , (GENRE.ACTN_RLS_MOVIE_CO)
     , (GENRE.ROMC_RLS_MOVIE_CO)
     , (GENRE.DCMTY_RLS_MOVIE_CO)
     , (GENRE.THLR_RLS_MOVIE_CO)
     , (GENRE.FAM_RLS_MOVIE_CO)
     , (GENRE.ADVT_RLS_MOVIE_CO)
     , (GENRE.FANTY_RLS_MOVIE_CO)
     , (GENRE.MYSTY_RLS_MOVIE_CO)
     , (GENRE.PBLPRFR_RLS_MOVIE_CO)
     , (GENRE.SF_RLS_MOVIE_CO)
     , (GENRE.MUSICL_RLS_MOVIE_CO)
     , (GENRE.HISTY_RLS_MOVIE_CO)
     , (GENRE.WAR_RLS_MOVIE_CO)
     , (GENRE.WT_RLS_MOVIE_CO)
     , (GENRE.ADULT_RLS_MOVIE_CO)
     , (GENRE.ETC_RLS_MOVIE_CO)
     , (DATA.METRP_AREA_AT)
FROM ((SELECT T.BASE_DE                         AS BASE_DE
            , T.CTPRVN_CD                       as CTPRVN_CD
            , T.CTPRVN_NM                       AS CTPRVN_NM
            , T.SCRNG_MOVIE_CO                  AS SCRNG_MOVIE_CO
            , M.RLS_MOVIE_CO                    AS RLS_MOVIE_CO
            , IF(T.SCRNG_MOVIE_CO = 0, 0,
                 M.RLS_MOVIE_CO / T.SCRNG_MOVIE_CO *
                 100)                           AS RLS_MOVIE_RT
            , (SELECT METRP_AT
               FROM ctprvn_info AS P
               WHERE T.CTPRVN_CD = P.CTPRVN_CD) AS METRP_AREA_AT
       FROM colct_movie_ctprvn_accto_stats AS T
                JOIN colct_movie_sales_stats AS M
                     ON T.BASE_DE = M.BASE_DE
       WHERE M.BASE_DE = ?)
      UNION ALL
      (SELECT BASE_DE
            , '00'    AS CTPRVN_CD
            , '전국'    AS CTPRVN_NM
            , SCRNG_MOVIE_CO
            , RLS_MOVIE_CO
            , IF(SCRNG_MOVIE_CO = 0, 0,
                 RLS_MOVIE_CO / SCRNG_MOVIE_CO *
                 100) AS RLS_MOVIE_RT
            , 'N'     AS METRP_AREA_AT
       FROM colct_movie_sales_stats
       WHERE BASE_DE = ?)) AS DATA
         JOIN (SELECT BASE_DE
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
               FROM (SELECT RLS_YEAR AS BASE_DE
                          , REPRSNT_GENRE_NM
                          , COUNT(*) AS MOVIE_CO
                     FROM colct_movie_info
                     WHERE REPRSNT_GENRE_NM is not null
                       and REPRSNT_GENRE_NM != ''
                       and RLS_YEAR = ?
                     GROUP BY RLS_YEAR, REPRSNT_GENRE_NM) AS MOVIE
               GROUP BY BASE_DE) AS GENRE
              ON DATA.BASE_DE = GENRE.BASE_DE;