INSERT INTO
	analysis_model.lsr_mvmn_qy_info
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD,
 CTPRVN_NM, DSTRCT_TY_CD, DSTRCT_TY_NM, MVMN_QY,
 MVMN_QY_IRDS_RT)
SELECT
	a.BASE_DE
, a.BASE_YEAR
, a.BASE_MT
, a.BASE_DAY
, a.CTPRVN_CD
, a.CTPRVN_NM
, a.DSTRCT_TY_CD
, a.DSTRCT_TY_NM
, a.MVMN_QY
, (a.MVMN_QY - b.MVMN_QY) / b.MVMN_QY *
  100 as MVMN_QY_IRDS_RT
FROM analysis_model.lsr_mvmn_qy_info AS a
JOIN analysis_model.lsr_mvmn_qy_info AS b
     ON a.CTPRVN_CD =
        b.CTPRVN_CD AND
        DATE(a.BASE_DE) -
        INTERVAL 7 DAY =
        DATE(b.BASE_DE) AND
        a.DSTRCT_TY_CD =
        b.DSTRCT_TY_CD
ON DUPLICATE KEY UPDATE
	MVMN_QY_IRDS_RT = VALUES(MVMN_QY_IRDS_RT);