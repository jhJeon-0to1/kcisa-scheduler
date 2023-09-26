INSERT INTO COLCT_MOVIE_MT_ACCTO_SALES_STATS
(BASE_YM, BASE_YEAR, BASE_MT, RLS_MOVIE_CO, SCRNG_MOVIE_CO, EXPNDTR_PRICE, MOVIE_ADNC_CO, COLCT_YM)
VALUE (?, ?, ?, ?, ?, ?, ?, DATE_FORMAT(NOW(), '%Y%m'))
ON DUPLICATE KEY UPDATE
    RLS_MOVIE_CO = VALUES(RLS_MOVIE_CO),
    SCRNG_MOVIE_CO = VALUES(SCRNG_MOVIE_CO),
    EXPNDTR_PRICE = VALUES(EXPNDTR_PRICE),
    MOVIE_ADNC_CO = VALUES(MOVIE_ADNC_CO),
    UPDT_YM = DATE_FORMAT(NOW(), '%Y%m')
