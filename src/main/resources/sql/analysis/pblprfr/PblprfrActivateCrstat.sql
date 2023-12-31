INSERT INTO
	analysis_model.pblprfr_activate_crstat
(BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM,
 RASNG_CUTIN_CO, PBLPRFR_CO, VIEWING_NMPR_CO, EXPNDTR_PRICE,
 RASNG_CUTIN_RT, POPLTN_PER_VIEWING_NMPR_CO,
 VIEWING_NMPR_CO_SCORE, EXPNDTR_PRICE_SCORE, GNRLZ_SCORE,
 LRGE_THEAT_CO, MIDDL_THEAT_CO, SMALL_THEAT_CO,
 STDR_RASNG_CUTIN_CO, STDR_PBPRFR_CO, STDR_VIEWING_NMPR_CO,
 STDR_EXPNDTR_PRICE)
SELECT
	DATA.BASE_YM
, SUBSTR(DATA.BASE_YM, 1, 4)         AS BASE_YEAR
, SUBSTR(DATA.BASE_YM, 5, 2)         AS BASE_MT
, DATA.CTPRVN_CD
, DATA.CTPRVN_NM
, DATA.PBLPRFR_RASNG_CUTIN_CO
, DATA.PBLPRFR_CO
, DATA.PBLPRFR_VIEWNG_NMPR_CO
, DATA.PBLPRFR_SALES_PRICE
, DATA.RASNG_CUTIN_RATE
, DATA.POPLTN_PER_VIEWNG_NMPR_CO
, (IF(STD.POPLTN_PER_VIEWNG_NMPR_CO = 0, NULL,
      DATA.POPLTN_PER_VIEWNG_NMPR_CO /
      STD.POPLTN_PER_VIEWNG_NMPR_CO *
      100))                          AS VIEWNG_NMPR_CO_SCORE
, (IF(STD.POPLTN_PER_EXPNDTR_PRICE = 0, null,
      DATA.POPLTN_PER_EXPNDTR_PRICE /
      STD.POPLTN_PER_EXPNDTR_PRICE *
      100))                          AS EXPNDTR_PRICE_SCORE
, (IF(STD.POPLTN_PER_VIEWNG_NMPR_CO = 0, NULL,
      DATA.POPLTN_PER_VIEWNG_NMPR_CO /
      STD.POPLTN_PER_VIEWNG_NMPR_CO *
      100) +
   IF(STD.POPLTN_PER_EXPNDTR_PRICE = 0, null,
      DATA.POPLTN_PER_EXPNDTR_PRICE /
      STD.POPLTN_PER_EXPNDTR_PRICE *
      100)) / 2                      AS GNRLZ_SCORE
, (SELECT LRGE_THEAT_CO
   FROM analysis_model.pblprfr_fclty_crstat AS F
   WHERE
	     DATA.BASE_YM = F.BASE_YM
   AND DATA.CTPRVN_CD = F.CTPRVN_CD) AS LRGE_THEAT_CO
, (SELECT MIDDL_THEAT_CO
   FROM analysis_model.pblprfr_fclty_crstat AS F
   WHERE
	     DATA.BASE_YM = F.BASE_YM
   AND DATA.CTPRVN_CD = F.CTPRVN_CD) AS MIDDL_THEAT_CO
, (SELECT SMALL_THEAT_CO
   FROM analysis_model.pblprfr_fclty_crstat AS F
   WHERE
	     DATA.BASE_YM = F.BASE_YM
   AND DATA.CTPRVN_CD = F.CTPRVN_CD) AS SMALL_THEAT_CO
, STD.PBLPRFR_RASNG_CUTIN_CO
, STD.PBLPRFR_CO
, STD.PBLPRFR_VIEWNG_NMPR_CO
, STD.PBLPRFR_SALES_PRICE
FROM (SELECT
	      LOCAL.BASE_YM
      , LOCAL.CTPRVN_CD
      , LOCAL.CTPRVN_NM
      , LOCAL.PBLPRFR_RASNG_CUTIN_CO
      , LOCAL.PBLPRFR_CO
      , LOCAL.PBLPRFR_VIEWNG_NMPR_CO
      , LOCAL.PBLPRFR_SALES_PRICE
      , LOCAL.RASNG_CUTIN_RATE
      , LOCAL.PBLPRFR_VIEWNG_NMPR_CO * 1000 /
        POP.POPLTN_CO AS POPLTN_PER_VIEWNG_NMPR_CO
      , LOCAL.PBLPRFR_SALES_PRICE / POP.POPLTN_CO *
        1000          AS POPLTN_PER_EXPNDTR_PRICE
      FROM (SELECT
	            BASE_YM
            , CTPRVN_CD
            , (SELECT CTPRVN_NM
               FROM ctprvn_info AS C
               WHERE
	               C.CTPRVN_CD =
	               T.CTPRVN_CD)           AS CTPRVN_NM
            , SUM(RASNG_CUTIN_CO)       AS PBLPRFR_RASNG_CUTIN_CO
            , SUM(PBLPRFR_CO)           AS PBLPRFR_CO
            , SUM(VIEWNG_NMPR_CO)       AS PBLPRFR_VIEWNG_NMPR_CO
            , SUM(EXPNDTR_PRICE) * 1000 AS PBLPRFR_SALES_PRICE
            , SUM(RASNG_CUTIN_CO) /
              SUM(PBLPRFR_CO) *
              100                       AS RASNG_CUTIN_RATE
            FROM colct_pblprfr_viewng_mt_accto_ctprvn_accto_stats AS T
            WHERE
	            BASE_YM = ?
            GROUP BY
	            BASE_YM, CTPRVN_CD)   AS LOCAL
      JOIN ctprvn_accto_popltn_info AS POP
           ON LOCAL.CTPRVN_CD = POP.CTPRVN_CD
	           AND LOCAL.BASE_YM = POP.BASE_YM
      UNION ALL
      SELECT
	      NATION.BASE_YM
      , NATION.CTPRVN_CD
      , NATION.CTPRVN_NM
      , NATION.PBLPRFR_RASNG_CUTIN_CO
      , NATION.PBLPRFR_CO
      , NATION.PBLPRFR_VIEWNG_NMPR_CO
      , NATION.PBLPRFR_SALES_PRICE
      , NATION.RASNG_CUTIN_RATE
      , NATION.PBLPRFR_VIEWNG_NMPR_CO * 1000 /
        POP.POPLTN_CO AS POPLTN_PER_VIEWNG_NMPR_CO
      , NATION.PBLPRFR_SALES_PRICE / POP.POPLTN_CO *
        1000          AS POPLTN_PER_EXPNDTR_PRICE
      FROM (SELECT
	            BASE_YM
            , '00'                      AS CTPRVN_CD
            , '전국'                      AS CTPRVN_NM
            , SUM(RASNG_CUTIN_CO)       AS PBLPRFR_RASNG_CUTIN_CO
            , SUM(PBLPRFR_CO)           AS PBLPRFR_CO
            , SUM(VIEWNG_NMPR_CO)       AS PBLPRFR_VIEWNG_NMPR_CO
            , SUM(EXPNDTR_PRICE) * 1000 AS PBLPRFR_SALES_PRICE
            , SUM(RASNG_CUTIN_CO) /
              SUM(PBLPRFR_CO) *
              100                       AS RASNG_CUTIN_RATE
            FROM colct_pblprfr_viewng_mt_accto_ctprvn_accto_stats
            WHERE
	            BASE_YM = ?
            GROUP BY
	            BASE_YM)              AS NATION
      JOIN ctprvn_accto_popltn_info AS POP
           ON
	           NATION.BASE_YM = POP.BASE_YM
		           AND POP.CTPRVN_CD = '00')        AS DATA
JOIN (SELECT
	      SEOUL.PBLPRFR_RASNG_CUTIN_CO
      , SEOUL.PBLPRFR_CO
      , SEOUL.PBLPRFR_VIEWNG_NMPR_CO
      , SEOUL.PBLPRFR_SALES_PRICE
      , SEOUL_POP.POPLTN
      , SEOUL.PBLPRFR_VIEWNG_NMPR_CO * 1000 /
        SEOUL_POP.POPLTN AS POPLTN_PER_VIEWNG_NMPR_CO
      , SEOUL.PBLPRFR_SALES_PRICE /
        SEOUL_POP.POPLTN *
        1000             AS POPLTN_PER_EXPNDTR_PRICE
      FROM (SELECT
	            SUM(RASNG_CUTIN_CO) / 12       AS PBLPRFR_RASNG_CUTIN_CO
            , SUM(PBLPRFR_CO) / 12           AS PBLPRFR_CO
            , SUM(VIEWNG_NMPR_CO) / 12       AS PBLPRFR_VIEWNG_NMPR_CO
            , SUM(EXPNDTR_PRICE) * 1000 / 12 AS PBLPRFR_SALES_PRICE
            FROM colct_pblprfr_viewng_mt_accto_ctprvn_accto_stats
            WHERE
	              BASE_YEAR = '2022'
            AND CTPRVN_CD = '11'
            GROUP BY
	            CTPRVN_CD)          AS SEOUL
      JOIN (SELECT
	            AVG(POPLTN_CO) AS POPLTN
            FROM ctprvn_accto_popltn_info
            WHERE
	              BASE_YEAR = '2022'
            AND CTPRVN_CD = '11') AS SEOUL_POP) AS STD
     ON 1 = 1
ON DUPLICATE KEY UPDATE
	                 BASE_YM                    = VALUES(BASE_YM)
                 , BASE_YEAR                  = VALUES(BASE_YEAR)
                 , BASE_MT                    = VALUES(BASE_MT)
                 , CTPRVN_CD                  = VALUES(CTPRVN_CD)
                 , CTPRVN_NM                  = VALUES(CTPRVN_NM)
                 , RASNG_CUTIN_CO             = VALUES(RASNG_CUTIN_CO)
                 , PBLPRFR_CO                 = VALUES(PBLPRFR_CO)
                 , VIEWING_NMPR_CO            = VALUES(VIEWING_NMPR_CO)
                 , EXPNDTR_PRICE              = VALUES(EXPNDTR_PRICE)
                 , RASNG_CUTIN_RT             = VALUES(RASNG_CUTIN_RT)
                 , POPLTN_PER_VIEWING_NMPR_CO = VALUES(POPLTN_PER_VIEWING_NMPR_CO)
                 , VIEWING_NMPR_CO_SCORE      = VALUES(VIEWING_NMPR_CO_SCORE)
                 , EXPNDTR_PRICE_SCORE        = VALUES(EXPNDTR_PRICE_SCORE)
                 , GNRLZ_SCORE                = VALUES(GNRLZ_SCORE)
                 , LRGE_THEAT_CO              = VALUES(LRGE_THEAT_CO)
                 , MIDDL_THEAT_CO             = VALUES(MIDDL_THEAT_CO)
                 , SMALL_THEAT_CO             = VALUES(SMALL_THEAT_CO)
                 , STDR_RASNG_CUTIN_CO        = VALUES(STDR_RASNG_CUTIN_CO)
                 , STDR_PBPRFR_CO             = VALUES(STDR_PBPRFR_CO)
                 , STDR_VIEWING_NMPR_CO       = VALUES(STDR_VIEWING_NMPR_CO)
                 , STDR_EXPNDTR_PRICE         = VALUES(STDR_EXPNDTR_PRICE)
;