INSERT INTO pet_activate_crstat
(BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM,
 PET_REGIST_CO, PET_RELATE_FCLTY_CO, PET_REGIST_SCORE,
 PET_RELATE_FCLTY_SCORE, GNRLZ_SCORE,
 POPLTN_PER_PET_REGIST_CO, POPLTN_PER_PET_RELATE_FCLTY_CO,
 STDR_PET_REGIST_CO, STDR_PET_RELATE_FCLTY_CO)
SELECT BASE_YM
     , BASE_YEAR
     , BASE_MT
     , CTPRVN_CD
     , CTPRVN_NM
     , PET_REGIST_CO
     , PET_RELATE_FCLTY_CO
     , PET_REGIST_SCORE
     , PET_RELATE_FCLTY_SCORE
     , ((PET_REGIST_SCORE + PET_RELATE_FCLTY_SCORE) /
        2) as GNRLZ_SCORE
     , POPLTN_PER_PET_REGIST_CO
     , POPLTN_PER_PET_RELATE_FCLTY_CO
     , STDR_PET_REGIST_CO
     , STDR_PET_RELATE_FCLTY_CO
FROM (SELECT DATA.BASE_YM
           , DATA.BASE_YEAR
           , DATA.BASE_MT                                 as BASE_MT
           , DATA.CTPRVN_CD                               as CTPRVN_CD
           , DATA.CTPRVN_NM                               as CTPRVN_NM
           , DATA.PET_REGIST_CO                           as PET_REGIST_CO
           , DATA.PET_RELATE_FCLTY_CO                     as PET_RELATE_FCLTY_CO
           , (DATA.POPLTN_PER_PET_REGIST_CO /
              SEOUL.POPLTN_PER_PET_REGIST_CO *
              100)                                        as PET_REGIST_SCORE
           , (DATA.POPLTN_PER_PET_RELATE_FCLTY_CO /
              SEOUL.POPLTN_PER_PET_RELATE_FCLTY_CO *
              100)                                        as PET_RELATE_FCLTY_SCORE
           , DATA.POPLTN_PER_PET_REGIST_CO
           , DATA.POPLTN_PER_PET_RELATE_FCLTY_CO
           , SEOUL.POPLTN_PER_PET_REGIST_CO               as STDR_PET_REGIST_CO
           , SEOUL.POPLTN_PER_PET_RELATE_FCLTY_CO         as STDR_PET_RELATE_FCLTY_CO
      FROM pet_ctprvn_accto_crstat as DATA
               JOIN (SELECT BASE_YM
                          , PET_REGIST_CO
                          , PET_RELATE_FCLTY_CO
                          , POPLTN_PER_PET_REGIST_CO
                          , POPLTN_PER_PET_RELATE_FCLTY_CO
                     FROM pet_ctprvn_accto_crstat
                     WHERE CTPRVN_CD = '11'
                       AND BASE_YM = ?) AS SEOUL
                    ON DATA.BASE_YM = SEOUL.BASE_YM
      WHERE DATA.BASE_YM = ?) AS data