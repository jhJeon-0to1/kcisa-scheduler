INSERT INTO sports_viewng_crstat
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD,
 CTPRVN_NM, SPORTS_VIEWNG_NMPR_CO,
 POPLTN_PER_VIEWNG_NMPR_CO, KLEA_VIEWNG_NMPR_CO,
 KBO_VIEWNG_NMPR_CO, KBL_VIEWNG_NMPR_CO,
 WKBL_VIEWNG_NMPR_CO, KOVO_VIEWNG_NMPR_CO)
SELECT BASE_DE
     , BASE_YEAR
     , BASE_MT
     , BASE_DAY
     , CTPRVN_CD
     , (SELECT CTPRVN_NM
        FROM ctprvn_info AS C
        WHERE C.CTPRVN_CD = S.CTPRVN_CD) AS CTPRVN_NM
     , SPORTS_VIEWNG_NMPR_CO
     , SPORTS_VIEWNG_NMPR_CO * 1000 / IFNULL(
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
                                      )  AS POPLTN_PER_VIEWNG_NMPR_CO
     , KLEA_VIEWNG_NMPR_CO
     , KBO_VIEWNG_NMPR_CO
     , KBL_VIEWNG_NMPR_CO
     , WKBL_VIEWNG_NMPR_CO
     , KOVO_VIEWNG_NMPR_CO
FROM colct_sports_viewng_info AS S
WHERE BASE_DE = ?
UNION ALL
SELECT BASE_DE
     , MAX(BASE_YEAR)                        as BASE_YEAR
     , MAX(BASE_MT)                          as BASE_MT
     , MAX(BASE_DAY)                         as BASE_DAY
     , '00'                                  as CTPRVN_CD
     , '전국'                                  as CTPRVN_NM
     , SUM(SPORTS_VIEWNG_NMPR_CO)            AS SPORTS_VIEWNG_NMPR_CO
     , SUM(SPORTS_VIEWNG_NMPR_CO) * 1000 / IFNULL(
        (SELECT POPLTN_CO
         FROM ctprvn_accto_popltn_info AS PP
         WHERE PP.CTPRVN_CD = '00'
           AND PP.BASE_YM = SUBSTR(BASE_DE, 1, 6))
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
WHERE BASE_DE = ?
GROUP BY BASE_DE
