INSERT INTO
    analysis_model.sports_match_info
(MATCH_SEQ_NO, MATCH_DE, BASE_YEAR, BASE_MT, BASE_DAY,
 GRP_NM, STDM_NM, HOME_TEAM_NM, AWAY_TEAM_NM, LEA_NM,
 SPORTS_VIEWNG_NMPR_CO)
SELECT
    MATCH_SEQ_NO
  , MATCH_DE
  , BASE_YEAR
  , BASE_MT
  , BASE_DAY
  , GRP_NM
  , STDM_NM
  , HOME_TEAM_NM
  , AWAY_TEAM_NM
  , LEA_NM
  , SPORTS_VIEWNG_NMPR_CO
FROM colct_sports_match_info
WHERE
    MATCH_DE = ?
ON DUPLICATE KEY UPDATE
                     MATCH_DE              = VALUES(MATCH_DE)
                   , BASE_YEAR             = VALUES(BASE_YEAR)
                   , BASE_MT               = VALUES(BASE_MT)
                   , BASE_DAY              = VALUES(BASE_DAY)
                   , GRP_NM                = VALUES(GRP_NM)
                   , STDM_NM               = VALUES(STDM_NM)
                   , HOME_TEAM_NM          = VALUES(HOME_TEAM_NM)
                   , AWAY_TEAM_NM          = VALUES(AWAY_TEAM_NM)
                   , LEA_NM                = VALUES(LEA_NM)
                   , SPORTS_VIEWNG_NMPR_CO = VALUES(SPORTS_VIEWNG_NMPR_CO)
