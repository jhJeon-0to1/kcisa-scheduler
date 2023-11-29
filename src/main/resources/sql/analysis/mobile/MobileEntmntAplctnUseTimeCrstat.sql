insert into
    analysis_model.mobile_entmnt_aplctn_use_time_crstat
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, WKDAY_NM,
 ENTMNT_CTGRY_NM, AVRG_USE_TIME)
SELECT
    BASE_DE
  , BASE_YEAR
  , BASE_MT
  , BASE_DAY
  , (case weekday(BASE_DE)
         when 0 then '월'
         when 1 then '화'
         when 2 then '수'
         when 3 then '목'
         when 4 then '금'
         when 5 then '토'
         when 6 then '일'
    end)               as WKDAY
  , LWPRT_CTGRY_NM     as ENTMNT_CTGRY_NM
  , ALL_USE_TIME /
    ALL_EMPR_CO / 3600 as AVRG_USE_TIME
FROM
    colct_mobile_ctgry_use_qy_info
WHERE
      UPPER_CTGRY_NM = '엔터테인먼트'
  AND LWPRT_CTGRY_NM <> ''
  AND BASE_DE = ?
UNION ALL
SELECT
    BASE_DE
  , MAX(BASE_YEAR)
  , MAX(BASE_MT)
  , MAX(BASE_DAY)
  , (case weekday(BASE_DE)
         when 0 then '월'
         when 1 then '화'
         when 2 then '수'
         when 3 then '목'
         when 4 then '금'
         when 5 then '토'
         when 6 then '일'
    end)                      as WKDAY
  , '전체'                      as ENTMNT_CTGRY_NM
  , SUM((ALL_USE_TIME /
         ALL_EMPR_CO / 3600)) as AVRG_USE_TIME
FROM
    colct_mobile_ctgry_use_qy_info
WHERE
      UPPER_CTGRY_NM = '엔터테인먼트'
  AND LWPRT_CTGRY_NM <> ''
  AND BASE_DE = ?
GROUP BY
    BASE_DE
ON DUPLICATE KEY UPDATE
    AVRG_USE_TIME = VALUES(AVRG_USE_TIME)
;