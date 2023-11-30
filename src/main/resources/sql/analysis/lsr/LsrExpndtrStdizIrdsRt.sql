INSERT INTO
    analysis_model.lsr_expndtr_stdiz_info
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD,
 CTPRVN_NM, INDUTY_TY_CD, INDUTY_TY_NM, FLCTTN_RT,
 FLCTTN_RT_IRDS_VALUE)
SELECT
    a.BASE_DE
  , a.BASE_YEAR
  , a.BASE_MT
  , a.BASE_DAY
  , a.CTPRVN_CD
  , a.CTPRVN_NM
  , a.INDUTY_TY_CD
  , a.INDUTY_TY_NM
  , a.FLCTTN_RT
  , a.FLCTTN_RT - b.FLCTTN_RT as FLCTTN_RT_IRDS_VALUE
FROM lsr_expndtr_stdiz_info as a
JOIN lsr_expndtr_stdiz_info as b
     ON a.CTPRVN_CD = b.CTPRVN_CD AND
        DATE(a.BASE_DE) - INTERVAL 7 DAY =
        DATE(b.BASE_DE) AND
        a.INDUTY_TY_CD = b.INDUTY_TY_CD
ON DUPLICATE KEY UPDATE
    FLCTTN_RT_IRDS_VALUE = VALUES(FLCTTN_RT_IRDS_VALUE);