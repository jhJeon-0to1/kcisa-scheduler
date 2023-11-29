INSERT INTO
    analysis_model.pblprfr_fclty_crstat
(BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM,
 LRGE_THEAT_CO, MIDDL_THEAT_CO, SMALL_THEAT_CO)
SELECT
    BASE_YM
  , substr(BASE_YM, 1, 4)                as BASE_YEAR
  , SUBSTR(BASE_YM, 5, 2)                as BASE_MT
  , CTPRVN_CD
  , MAX(CTPRVN_NM)                       as CTPRVN_NM
  , SUM(IF(FCLTY_SEAT_CO >= 1000, 1, 0)) as LRGE_THEAT_CO
  , SUM(IF(FCLTY_SEAT_CO > 300 and FCLTY_SEAT_CO < 1000,
           1,
           0))                           as MIDDL_THEAT_CO
  , SUM(IF(FCLTY_SEAT_CO <= 300, 1, 0))  as SMALL_THEAT_CO
FROM
    (SELECT
         ? as BASE_YM
       , A.CTPRVN_CD
       , A.CTPRVN_NM
       , B.FCLTY_SEAT_CO
     FROM
         colct_pblprfr_fclty_info as A
             JOIN colct_pblprfr_fclty_detail_info as B
                  ON A.PBLPRFR_FCLTY_ID =
                     B.PBLPRFR_FCLTY_ID
                      AND A.COLCT_YM = B.COLCT_YM
     WHERE
         A.COLCT_YM = ?) AS DATA
GROUP BY
    BASE_YM, CTPRVN_CD
ON DUPLICATE KEY UPDATE
                     LRGE_THEAT_CO  = VALUES(LRGE_THEAT_CO)
                   , MIDDL_THEAT_CO = VALUES(MIDDL_THEAT_CO)
                   , SMALL_THEAT_CO = VALUES(SMALL_THEAT_CO)
