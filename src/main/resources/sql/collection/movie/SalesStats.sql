INSERT INTO colct_movie_sales_stats
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, RLS_MOVIE_CO,
 SCRNG_MOVIE_CO, EXPNDTR_PRICE, MOVIE_ADNC_CO, COLCT_DE)
    VALUE (?, ?, ?, ?, ?, ?, ?, ?,
           DATE_FORMAT(NOW(), '%Y%m%d'))
ON DUPLICATE KEY UPDATE RLS_MOVIE_CO   = VALUES(RLS_MOVIE_CO),
                        SCRNG_MOVIE_CO = VALUES(SCRNG_MOVIE_CO),
                        EXPNDTR_PRICE  = VALUES(EXPNDTR_PRICE),
                        MOVIE_ADNC_CO  = VALUES(MOVIE_ADNC_CO),
                        UPDT_DE        = DATE_FORMAT(NOW(), '%Y%m%d')
