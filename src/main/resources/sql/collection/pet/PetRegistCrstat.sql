INSERT INTO colct_pet_regist_crstat
(BASE_YM, CTPRVN_CD, CTPRVN_NM, PET_KND_CD, PET_KND_NM,
 PET_REGIST_CO, COLCT_DT)
VALUES (?, (SELECT CTPRVN_CD
            from ctprvn_info
            where PET_CTPRVN_NM = ?),
        (SELECT CTPRVN_NM
         from ctprvn_info
         where PET_CTPRVN_NM = ?), ?, ?, ?,
        NOW())
ON DUPLICATE KEY UPDATE PET_REGIST_CO = VALUES(PET_REGIST_CO)
                      , UPDT_DT       = NOW();