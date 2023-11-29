INSERT INTO
    analysis_model.mobile_entmnt_aplctn_mt_accto_use_time_crstat
(BASE_YM, BASE_YEAR, BASE_MT, ENTMNT_CTGRY_NM,
 AVRG_USE_TIME)
SELECT
    BASE_YM,
    SUBSTR(BASE_YM, 1, 4) AS BASE_YEAR,
    SUBSTR(BASE_YM, 5, 2) AS BASE_MT,
    ENTMNT_CTGRY_NM,
    SUM(AVRG_USE_TIME)    AS AVRG_USE_TIME
FROM
    (SELECT
         SUBSTR(BASE_DE, 1, 6)                  AS BASE_YM,
         LWPRT_CTGRY_NM                         AS ENTMNT_CTGRY_NM,
         SUM(ALL_USE_TIME / ALL_EMPR_CO / 3600) AS AVRG_USE_TIME
     FROM
         colct_mobile_ctgry_use_qy_info
     WHERE
           UPPER_CTGRY_NM = '엔터테인먼트'
       AND LWPRT_CTGRY_NM <> ''
       AND BASE_DE BETWEEN ? AND ?
     GROUP BY
         BASE_YM, LWPRT_CTGRY_NM

     UNION ALL

     SELECT
         SUBSTR(BASE_DE, 1, 6)                  AS BASE_YM,
         '전체'                                   AS ENTMNT_CTGRY_NM,
         SUM(ALL_USE_TIME / ALL_EMPR_CO / 3600) AS AVRG_USE_TIME
     FROM
         colct_mobile_ctgry_use_qy_info
     WHERE
           UPPER_CTGRY_NM = '엔터테인먼트'
       AND LWPRT_CTGRY_NM <> ''
       AND BASE_DE BETWEEN ? AND ?
     GROUP BY
         BASE_DE) AS T
GROUP BY
    BASE_YM, ENTMNT_CTGRY_NM
ON DUPLICATE KEY UPDATE
    AVRG_USE_TIME = VALUES(AVRG_USE_TIME)
;