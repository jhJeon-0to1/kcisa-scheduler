INSERT analysis_etl.colct_sports_viewng_info (BASE_DE,
                                              BASE_YEAR,
                                              BASE_MT,
                                              BASE_DAY,
                                              CTPRVN_CD,
                                              CTPRVN_NM,
                                              KLEA_VIEWNG_NMPR_CO,
                                              KBO_VIEWNG_NMPR_CO,
                                              KBL_VIEWNG_NMPR_CO,
                                              WKBL_VIEWNG_NMPR_CO,
                                              KOVO_VIEWNG_NMPR_CO,
                                              SPORTS_VIEWNG_NMPR_CO,
                                              KLEA_MATCH_CO,
                                              KBO_MATCH_CO,
                                              KBL_MATCH_CO,
                                              WKBL_MATCH_CO,
                                              KOVO_MATCH_CO,
                                              SPORTS_MATCH_CO,
                                              COLCT_DE) VALUE (?,
                                                               ?,
                                                               ?,
                                                               ?,
                                                               ?,
                                                               (SELECT CTPRVN_NM
                                                                FROM ctprvn_info AS C
                                                                WHERE C.CTPRVN_CD = ?),
                                                               ?,
                                                               ?,
                                                               ?,
                                                               ?,
                                                               ?,
                                                               ?,
                                                               ?,
                                                               ?,
                                                               ?,
                                                               ?,
                                                               ?,
                                                               ?,
                                                               DATE_FORMAT(NOW(), '%Y%m%d'))
ON DUPLICATE KEY UPDATE BASE_DE               = VALUES(BASE_DE),
                        BASE_YEAR             = VALUES(BASE_YEAR),
                        BASE_MT               = VALUES(BASE_MT),
                        BASE_DAY              = VALUES(BASE_DAY),
                        CTPRVN_CD             = VALUES(CTPRVN_CD),
                        KLEA_VIEWNG_NMPR_CO   = VALUES(KLEA_VIEWNG_NMPR_CO),
                        KBO_VIEWNG_NMPR_CO    = VALUES(KBO_VIEWNG_NMPR_CO),
                        KBL_VIEWNG_NMPR_CO    = VALUES(KBL_VIEWNG_NMPR_CO),
                        WKBL_VIEWNG_NMPR_CO   = VALUES(WKBL_VIEWNG_NMPR_CO),
                        KOVO_VIEWNG_NMPR_CO   = VALUES(KOVO_VIEWNG_NMPR_CO),
                        SPORTS_VIEWNG_NMPR_CO = VALUES(SPORTS_VIEWNG_NMPR_CO),
                        KLEA_MATCH_CO         = VALUES(KLEA_MATCH_CO),
                        KBO_MATCH_CO          = VALUES(KBO_MATCH_CO),
                        KBL_MATCH_CO          = VALUES(KBL_MATCH_CO),
                        WKBL_MATCH_CO         = VALUES(WKBL_MATCH_CO),
                        KOVO_MATCH_CO         = VALUES(KOVO_MATCH_CO),
                        SPORTS_MATCH_CO       = VALUES(SPORTS_MATCH_CO),
                        UPDT_DE               = DATE_FORMAT(NOW(), '%Y%m%d')