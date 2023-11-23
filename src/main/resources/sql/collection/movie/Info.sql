INSERT INTO colct_movie_info
(MOVIE_CD, MOVIE_NM, MOVIE_ENG_NM, MNFCT_YEAR, RLS_YEAR,
 COUNTY_NM, GENRE_NM, REPRSNT_COUNTY_NM, REPRSNT_GENRE_NM,
 COLCT_DE)
    VALUE (?, ?, ?, ?, ?, ?, ?, ?, ?,
           DATE_FORMAT(NOW(), '%Y%m%d'))
ON DUPLICATE KEY UPDATE MOVIE_NM          = VALUES(MOVIE_NM),
                        MOVIE_ENG_NM      = VALUES(MOVIE_ENG_NM),
                        MNFCT_YEAR        = VALUES(MNFCT_YEAR),
                        RLS_YEAR          = VALUES(RLS_YEAR),
                        COUNTY_NM         = VALUES(COUNTY_NM),
                        GENRE_NM          = VALUES(GENRE_NM),
                        REPRSNT_COUNTY_NM = VALUES(REPRSNT_COUNTY_NM),
                        REPRSNT_GENRE_NM  = VALUES(REPRSNT_GENRE_NM),
                        UPDT_DE           = DATE_FORMAT(NOW(), '%Y%m%d')
