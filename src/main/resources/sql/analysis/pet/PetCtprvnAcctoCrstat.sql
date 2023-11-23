INSERT INTO pet_ctprvn_accto_crstat
(BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM,
 PET_REGIST_CO, DOG_REGIST_CO, CAT_REGIST_CO,
 POPLTN_PER_PET_REGIST_CO, POPLTN_PER_DOG_REGIST_CO,
 POPLTN_PER_CAT_REGIST_CO, PET_HSPT_CO, PET_BTY_FCLTY_CO,
 PET_CONSGN_MANAGE_FCLTY_CO, PET_ACP_CTLSTT_CO,
 PET_RELATE_FCLTY_CO, POPLTN_PER_PET_HSPT_CO,
 POPLTN_PER_PET_BTY_FCLTY_CO,
 POPLTN_PER_PET_CONSGN_MANAGE_FCLTY_CO,
 POPLTN_PER_PET_ACP_CTLSTT_CO,
 POPLTN_PER_PET_RELATE_FCLTY_CO)
SELECT R2.BASE_YM
     , SUBSTR(R2.BASE_YM, 1, 4)                           AS BASE_YEAR
     , SUBSTR(R2.BASE_YM, 5, 2)                           AS BASE_MT
     , R2.CTPRVN_CD
     , R2.CTPRVN_NM
     , R2.PET_REGIST_CO
     , R2.DOG_REGIST_CO
     , R2.CAT_REGIST_CO
     , R2.PET_REGIST_CO * 1000 /
       R2.POPLTN_CO                                       AS POPLTN_PER_PET_REGIST_CO
     , R2.DOG_REGIST_CO * 1000 /
       R2.POPLTN_CO                                       AS POPLTN_PER_DOG_REGIST_CO
     , R2.CAT_REGIST_CO * 1000 /
       R2.POPLTN_CO                                       AS POPLTN_PER_CAT_REGIST_CO
     , H.PET_HSPT_CO
     , B.PET_BTY_FCLTY_CO
     , C.PET_CONSGN_MANAGE_FCLTY_CO
     , A.PET_ACP_CTLSTT_CO
     , (H.PET_HSPT_CO + B.PET_BTY_FCLTY_CO +
        C.PET_CONSGN_MANAGE_FCLTY_CO +
        A.PET_ACP_CTLSTT_CO)                              AS PET_RELATE_FCLTY_CO
     , H.PET_HSPT_CO * 1000 / R2.POPLTN_CO                AS POPLTN_PER_PET_HSPT_CO
     , B.PET_BTY_FCLTY_CO * 1000 /
       R2.POPLTN_CO                                       AS POPLTN_PER_PET_BTY_FCLTY_CO
     , C.PET_CONSGN_MANAGE_FCLTY_CO * 1000 /
       R2.POPLTN_CO                                       AS POPLTN_PER_PET_CONSGN_MANAGE_FCLTY_CO
     , A.PET_ACP_CTLSTT_CO * 1000 /
       R2.POPLTN_CO                                       AS POPLTN_PER_PET_ACP_CTLSTT_CO
     , (H.PET_HSPT_CO + B.PET_BTY_FCLTY_CO +
        C.PET_CONSGN_MANAGE_FCLTY_CO +
        A.PET_ACP_CTLSTT_CO) * 1000 /
       R2.POPLTN_CO                                       AS POPLTN_PER_PET_RELATE_FCLTY_CO
FROM (SELECT BASE_YM
           , CTPRVN_CD
           , MAX(CTPRVN_NM)                               AS CTPRVN_NM
           , SUM(PET_REGIST_CO)                           AS PET_REGIST_CO
           , SUM(IF(PET_KND_CD = '01', PET_REGIST_CO,
                    0))                                   AS DOG_REGIST_CO
           , SUM(IF(PET_KND_CD = '02', PET_REGIST_CO,
                    0))                                   AS CAT_REGIST_CO
           , (SELECT POPLTN_CO
              FROM ctprvn_accto_popltn_info AS P
              WHERE P.BASE_YM = R1.BASE_YM
                AND P.CTPRVN_CD = R1.CTPRVN_CD)           AS POPLTN_CO
      FROM colct_pet_regist_crstat AS R1
      WHERE BASE_YM = ?
      GROUP BY BASE_YM, CTPRVN_CD) AS R2
         JOIN (SELECT BASE_YM
                    , CTPRVN_CD
                    , MAX(CTPRVN_NM) AS CTPRVN_NM
                    , PET_HSPT_CO
               FROM colct_pet_hspt_license_info
               WHERE BASE_YM = ?
               GROUP BY BASE_YM, CTPRVN_CD) AS H
              ON R2.BASE_YM = H.BASE_YM AND
                 R2.CTPRVN_CD = H.CTPRVN_CD
         JOIN (SELECT BASE_YM
                    , CTPRVN_CD
                    , MAX(CTPRVN_NM) AS CTPRVN_NM
                    , PET_BTY_FCLTY_CO
               FROM colct_pet_bty_fclty_license_info
               WHERE BASE_YM = ?
               GROUP BY BASE_YM, CTPRVN_CD) AS B
              ON H.BASE_YM = B.BASE_YM AND
                 H.CTPRVN_CD = B.CTPRVN_CD
         JOIN (SELECT BASE_YM
                    , CTPRVN_CD
                    , MAX(CTPRVN_NM) AS CTPRVN_NM
                    , PET_CONSGN_MANAGE_FCLTY_CO
               FROM colct_pet_consgn_manage_fclty_license_info
               WHERE BASE_YM = ?
               GROUP BY BASE_YM, CTPRVN_CD) AS C
              ON H.BASE_YM = C.BASE_YM AND
                 H.CTPRVN_CD = C.CTPRVN_CD
         JOIN (SELECT BASE_YM
                    , CTPRVN_CD
                    , MAX(CTPRVN_NM) AS CTPRVN_NM
                    , PET_ACP_CTLSTT_CO
               FROM colct_pet_acp_ctlstt_info
               WHERE BASE_YM = '202309'
               GROUP BY BASE_YM, CTPRVN_CD) AS A
              ON H.CTPRVN_CD = A.CTPRVN_CD


