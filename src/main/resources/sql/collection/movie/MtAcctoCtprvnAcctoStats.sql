INSERT INTO colct_movie_mt_accto_ctprvn_accto_stats
(BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM,
 SCRNG_MOVIE_CO, EXPNDTR_PRICE, MOVIE_ADNC_CO, COLCT_YM)
    VALUE (?, ?, ?, ?, (SELECT CTPRVN_NM
                        FROM ctprvn_info AS C
                        WHERE C.CTPRVN_CD = ?), ?, ?, ?,
           DATE_FORMAT(NOW(), '%Y%m'))
ON DUPLICATE KEY UPDATE SCRNG_MOVIE_CO = VALUES(SCRNG_MOVIE_CO),
                        EXPNDTR_PRICE  = VALUES(EXPNDTR_PRICE),
                        MOVIE_ADNC_CO  = VALUES(MOVIE_ADNC_CO),
                        UPDT_YM        = DATE_FORMAT(NOW(), '%Y%m')