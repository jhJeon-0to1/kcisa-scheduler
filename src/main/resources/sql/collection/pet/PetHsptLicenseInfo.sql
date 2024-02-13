INSERT INTO colct_pet_hspt_license_info
(BASE_YM, CTPRVN_CD, CTPRVN_NM, PET_HSPT_CO, COLCT_DT)
VALUES (?,
        (SELECT CTPRVN_CD
         FROM ctprvn_info
         where PET_CTPRVN_NM = ?),
        (SELECT CTPRVN_NM
         FROM ctprvn_info
         WHERE PET_CTPRVN_NM = ?),
        ?, NOW())
ON DUPLICATE KEY UPDATE PET_HSPT_CO = VALUES(PET_HSPT_CO),
                        UPDT_DT     = NOW()
