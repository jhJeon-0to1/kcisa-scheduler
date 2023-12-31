INSERT INTO colct_movie_ctprvn_accto_stats
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD,
 CTPRVN_NM, SCRNG_MOVIE_CO, EXPNDTR_PRICE, MOVIE_ADNC_CO,
 COLCT_DE)
    VALUE (?, ?, ?, ?, ?, (SELECT CTPRVN_NM
                           FROM ctprvn_info AS C
                           WHERE C.CTPRVN_CD = ?), ?, ?, ?,
           DATE_FORMAT(NOW(), '%Y%m%d'))
ON DUPLICATE KEY UPDATE SCRNG_MOVIE_CO = VALUES(SCRNG_MOVIE_CO),
                        EXPNDTR_PRICE  = VALUES(EXPNDTR_PRICE),
                        MOVIE_ADNC_CO  = VALUES(MOVIE_ADNC_CO),
                        UPDT_DE        = DATE_FORMAT(NOW(), '%Y%m%d')