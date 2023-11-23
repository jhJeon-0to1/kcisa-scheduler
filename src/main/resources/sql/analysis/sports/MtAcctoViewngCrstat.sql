INSERT INTO sports_mt_accto_viewng_crstat
(BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM,
 SPORTS_VIEWNG_NMPR_CO, POPLTN_PER_VIEWNG_NMPR_CO,
 KLEA_VIEWNG_NMPR_CO, KBO_VIEWNG_NMPR_CO,
 KBL_VIEWNG_NMPR_CO, WKBL_VIEWNG_NMPR_CO,
 KOVO_VIEWNG_NMPR_CO)
SELECT CONCAT(BASE_YEAR, BASE_MT)            AS BASE_YM
     , BASE_YEAR
     , BASE_MT
     , CTPRVN_CD
     , (SELECT CTPRVN_NM
        FROM ctprvn_info AS C
        WHERE C.CTPRVN_CD = S.CTPRVN_CD)     AS CTPRVN_NM
     , SUM(SPORTS_VIEWNG_NMPR_CO)            AS SPORTS_VIEWNG_NMPR_CO
     , SUM(SPORTS_VIEWNG_NMPR_CO) * 1000 / IFNULL(
        (SELECT POPLTN_CO
         FROM ctprvn_accto_popltn_info AS PP
         WHERE PP.CTPRVN_CD = S.CTPRVN_CD
           AND PP.BASE_YM = CONCAT(S.BASE_YEAR, S.BASE_MT))
    , (SELECT POPLTN_CO
       FROM ctprvn_accto_popltn_info AS PP
       WHERE PP.CTPRVN_CD = S.CTPRVN_CD
         AND PP.BASE_YM = (SELECT MAX(BASE_YM) AS BASE_YM
                           FROM ctprvn_accto_popltn_info AS P
                           WHERE P.CTPRVN_CD = S.CTPRVN_CD))
                                           ) AS POPLTN_PER_VIEWNG_NMPR_CO
     , SUM(KLEA_VIEWNG_NMPR_CO)              AS KLEA_VIEWNG_NMPR_CO
     , SUM(KBO_VIEWNG_NMPR_CO)               AS KBO_VIEWNG_NMPR_CO
     , SUM(KBL_VIEWNG_NMPR_CO)               AS KBL_VIEWNG_NMPR_CO
     , SUM(WKBL_VIEWNG_NMPR_CO)              AS WKBL_VIEWNG_NMPR_CO
     , SUM(KOVO_VIEWNG_NMPR_CO)              AS KOVO_VIEWNG_NMPR_CO
FROM colct_sports_viewng_info AS S
WHERE BASE_YEAR = ?
  and BASE_MT = ?
GROUP BY BASE_YEAR, BASE_MT, CTPRVN_CD
UNION ALL
SELECT CONCAT(BASE_YEAR, BASE_MT)            AS BASE_YM
     , BASE_YEAR
     , BASE_MT
     , '00'                                  AS CTPRVN_CD
     , '전국'                                  AS CTPRVN_NM
     , SUM(SPORTS_VIEWNG_NMPR_CO)            AS SPORTS_VIEWNG_NMPR_CO
     , SUM(SPORTS_VIEWNG_NMPR_CO) * 1000 / IFNULL(
        (SELECT POPLTN_CO
         FROM ctprvn_accto_popltn_info AS PP
         WHERE PP.CTPRVN_CD = '00'
           AND PP.BASE_YM = CONCAT(S.BASE_YEAR, S.BASE_MT))
    , (SELECT POPLTN_CO
       FROM ctprvn_accto_popltn_info AS PP
       WHERE PP.CTPRVN_CD = '00'
         AND PP.BASE_YM = (SELECT MAX(BASE_YM) AS BASE_YM
                           FROM ctprvn_accto_popltn_info AS P
                           WHERE P.CTPRVN_CD = '00'))
                                           ) AS POPLTN_PER_VIEWNG_NMPR_CO
     , SUM(KLEA_VIEWNG_NMPR_CO)              AS KLEA_VIEWNG_NMPR_CO
     , SUM(KBO_VIEWNG_NMPR_CO)               AS KBO_VIEWNG_NMPR_CO
     , SUM(KBL_VIEWNG_NMPR_CO)               AS KBL_VIEWNG_NMPR_CO
     , SUM(WKBL_VIEWNG_NMPR_CO)              AS WKBL_VIEWNG_NMPR_CO
     , SUM(KOVO_VIEWNG_NMPR_CO)              AS KOVO_VIEWNG_NMPR_CO
FROM colct_sports_viewng_info AS S
WHERE BASE_YEAR = ?
  and BASE_MT = ?
GROUP BY BASE_YEAR, BASE_MT
