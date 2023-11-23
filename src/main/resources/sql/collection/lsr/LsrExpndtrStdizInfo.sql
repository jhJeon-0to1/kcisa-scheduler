INSERT INTO colct_lsr_expndtr_stdiz_info
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD,
 CTPRVN_NM, INDUTY_TY_CD, INDUTY_TY_NM, FLCTTN_RT, COLCT_DE)
VALUES (?, ?, ?, ?, ?, (SELECT CTPRVN_NM
                        FROM ctprvn_info
                        WHERE CTPRVN_CD = ?), ?, ?, ?,
        DATE_FORMAT(NOW(), '%Y%m%d'))
ON DUPLICATE KEY UPDATE BASE_YEAR    = VALUES(BASE_YEAR)
                      , BASE_MT      = VALUES(BASE_MT)
                      , BASE_DAY     = VALUES(BASE_DAY)
                      , CTPRVN_NM    = VALUES(CTPRVN_NM)
                      , INDUTY_TY_NM = VALUES(INDUTY_TY_NM)
                      , FLCTTN_RT    = VALUES(FLCTTN_RT)
                      , UPDT_DE      = DATE_FORMAT(NOW(), '%Y%m%d')
