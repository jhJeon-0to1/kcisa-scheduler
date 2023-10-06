INSERT INTO COLCT_PBLPRFR_VIEWNG_CTPRVN_ACCTO_STAT
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD, CTPRVN_NM, GENRE_CD, GENRE_NM, PBLPRFR_RASNG_CUTIN_CO, PBLPRFR_STGNG_CO, PBLPRFR_SALES_PRICE, PBLPRFR_VIEWNG_NMPR_CO, PBLPRFR_CO, COLCT_DE)
VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?, DATE_FORMAT(NOW(), '%Y%m%d'))
ON DUPLICATE KEY UPDATE
    BASE_YEAR = VALUES(BASE_YEAR)
    , BASE_MT = VALUES(BASE_MT)
    , BASE_DAY = VALUES(BASE_DAY)
    , CTPRVN_NM = VALUES(CTPRVN_NM)
    , GENRE_NM = VALUES(GENRE_NM)
    , PBLPRFR_RASNG_CUTIN_CO = VALUES(PBLPRFR_RASNG_CUTIN_CO)
    , PBLPRFR_CO = VALUES(PBLPRFR_CO)
    , PBLPRFR_STGNG_CO = VALUES(PBLPRFR_STGNG_CO)
    , PBLPRFR_SALES_PRICE = VALUES(PBLPRFR_SALES_PRICE)
    , PBLPRFR_VIEWNG_NMPR_CO = VALUES(PBLPRFR_VIEWNG_NMPR_CO)
    , UPDT_DE = DATE_FORMAT(NOW(), '%Y%m%d')