INSERT INTO colct_pblprfr_viewng_mt_accto_ctprvn_accto_stats
(BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM,
 GENRE_CD, GENRE_NM, PBLPRFR_RASNG_CUTIN_CO,
 PBLPRFR_STGNG_CO, PBLPRFR_SALES_PRICE,
 PBLPRFR_VIEWNG_NMPR_CO, PBLPRFR_CO, COLCT_YM)
    VALUE (?, ?, ?, ?, (SELECT CTPRVN_NM
                        FROM ctprvn_info
                        WHERE CTPRVN_CD = ?), ?, ?, ?, ?, ?,
           ?, ?, DATE_FORMAT(NOW(), '%Y%m'))
ON DUPLICATE KEY UPDATE BASE_YEAR              = VALUES(BASE_YEAR)
                      , BASE_MT                = VALUES(BASE_MT)
                      , CTPRVN_NM              = VALUES(CTPRVN_NM)
                      , GENRE_NM               = VALUES(GENRE_NM)
                      , PBLPRFR_RASNG_CUTIN_CO = VALUES(PBLPRFR_RASNG_CUTIN_CO)
                      , PBLPRFR_CO             = VALUES(PBLPRFR_CO)
                      , PBLPRFR_STGNG_CO       = VALUES(PBLPRFR_STGNG_CO)
                      , PBLPRFR_SALES_PRICE    = VALUES(PBLPRFR_SALES_PRICE)
                      , PBLPRFR_VIEWNG_NMPR_CO = VALUES(PBLPRFR_VIEWNG_NMPR_CO)
                      , UPDT_YM                = DATE_FORMAT(NOW(), '%Y%m')