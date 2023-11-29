INSERT INTO
    analysis_model.mobile_aplctn_mt_accto_use_time_crstat
(BASE_YM, BASE_YEAR, BASE_MT, CTGRY_NM, AVRG_USE_TIME)
SELECT
    BASE_YM
  , BASE_YEAR
  , BASE_MT
  , UPPER_CTGRY_NM
  , AVRG_USE_TIME
FROM
    (SELECT
         BASE_YM
       , SUBSTR(BASE_YM, 1, 4) as BASE_YEAR
       , SUBSTR(BASE_YM, 5, 2) as BASE_MT
       , UPPER_CTGRY_NM
       , SUM(AVRG_USE_TIME)    as AVRG_USE_TIME
     FROM
         (SELECT
              SUBSTR(BASE_DE, 1, 6)             as BASE_YM
            , UPPER_CTGRY_NM
            , ALL_USE_TIME / ALL_EMPR_CO / 3600 as AVRG_USE_TIME
          FROM
              colct_mobile_ctgry_use_qy_info
          WHERE
                  UPPER_CTGRY_NM not in ('전체', '엔터테인먼트')
            AND   BASE_DE BETWEEN ? AND ?) AS T1
     GROUP BY BASE_YM, UPPER_CTGRY_NM
     UNION ALL
     SELECT
         BASE_YM
       , BASE_YEAR
       , BASE_MT
       , '엔터테인먼트' AS UPPER_CTGRY_NM
       , AVRG_USE_TIME
     FROM
         mobile_entmnt_aplctn_mt_accto_use_time_crstat
     WHERE
           ENTMNT_CTGRY_NM = '전체'
       AND BASE_YM = ?) AS DATA
UNION ALL
SELECT
    BASE_YM
  , MAX(BASE_YEAR)     as BASE_YEAR
  , MAX(BASE_MT)       as BASE_MT
  , '전체'               as UPPER_CTGRY_NM
  , SUM(AVRG_USE_TIME) as AVRG_USE_TIME
FROM
    (SELECT
         BASE_YM
       , SUBSTR(BASE_YM, 1, 4) as BASE_YEAR
       , SUBSTR(BASE_YM, 5, 2) as BASE_MT
       , SUM(AVRG_USE_TIME)    as AVRG_USE_TIME
     FROM
         (SELECT
              SUBSTR(BASE_DE, 1, 6)             as BASE_YM
            , UPPER_CTGRY_NM
            , ALL_USE_TIME / ALL_EMPR_CO / 3600 as AVRG_USE_TIME
          FROM
              colct_mobile_ctgry_use_qy_info
          WHERE
                  UPPER_CTGRY_NM not in ('전체', '엔터테인먼트')
            AND   BASE_DE BETWEEN ? AND ?) AS T1
     GROUP BY BASE_YM, UPPER_CTGRY_NM
     UNION ALL
     SELECT
         BASE_YM
       , BASE_YEAR
       , BASE_MT
       , AVRG_USE_TIME
     FROM
         mobile_entmnt_aplctn_mt_accto_use_time_crstat
     WHERE
           ENTMNT_CTGRY_NM = '전체'
       AND BASE_YM = ?) AS TOTAL
GROUP BY
    BASE_YM
ON DUPLICATE KEY UPDATE
    AVRG_USE_TIME = VALUES(AVRG_USE_TIME)
