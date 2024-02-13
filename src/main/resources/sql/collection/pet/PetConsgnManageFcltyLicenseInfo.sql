INSERT INTO colct_pet_consgn_manage_fclty_license_info
(BASE_YM, CTPRVN_CD, CTPRVN_NM, PET_CONSGN_MANAGE_FCLTY_CO,
 COLCT_DT)
VALUES (?,
        (SELECT CTPRVN_CD
         FROM ctprvn_info
         where PET_CTPRVN_NM = ?),
        (SELECT CTPRVN_NM
         FROM ctprvn_info
         WHERE PET_CTPRVN_NM = ?),
        ?, NOW())
ON DUPLICATE KEY UPDATE PET_CONSGN_MANAGE_FCLTY_CO = VALUES(PET_CONSGN_MANAGE_FCLTY_CO),
                        UPDT_DT                    = NOW()
