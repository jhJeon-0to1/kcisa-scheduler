INSERT INTO colct_lsr_mvmn_qy_info
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD,
 CTPRVN_NM, DSTRCT_TY_CD, DSTRCT_TY_NM, MVMN_QY, COLCT_DE)
VALUES (?, ?, ?, ?, ?, (SELECT CTPRVN_NM
                        FROM ctprvn_info
                        WHERE CTPRVN_CD = ?), ?, ?, ?,
        DATE_FORMAT(NOW(), '%Y%m%d'))
ON DUPLICATE KEY UPDATE BASE_YEAR    = VALUES(BASE_YEAR)
                      , BASE_MT      = VALUES(BASE_MT)
                      , BASE_DAY     = VALUES(BASE_DAY)
                      , CTPRVN_NM    = VALUES(CTPRVN_NM)
                      , DSTRCT_TY_NM = VALUES(DSTRCT_TY_NM)
                      , MVMN_QY      = VALUES(MVMN_QY)
                      , UPDT_DE      = DATE_FORMAT(NOW(), '%Y%m%d')
