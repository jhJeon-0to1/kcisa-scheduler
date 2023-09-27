SELECT
    BASE_DE
    , T.CTPRVN_CD
    , T.CTPRVN_NM
    , T.MOVIE_ADNC_CO
    , (T.MOVIE_ADNC_CO * 1000 / (
        IFNULL(
            (
                SELECT
                    POPLTN_CO
                FROM CTPRVN_ACCTO_POPLTN_INFO AS PP
                WHERE PP.CTPRVN_CD = T.CTPRVN_CD
                    AND PP.BASE_YM = SUBSTR(T.BASE_DE, 1, 6)
            )
            , (
            SELECT
                POPLTN_CO
            FROM CTPRVN_ACCTO_POPLTN_INFO AS PP
            WHERE PP.CTPRVN_CD = T.CTPRVN_CD
                AND PP.BASE_YM = (
                    SELECT
                        MAX(BASE_YM) AS BASE_YM
                    FROM CTPRVN_ACCTO_POPLTN_INFO AS P
                    WHERE P.CTPRVN_CD = T.CTPRVN_CD
                )
            )
        )
    )) AS POPULATION_PER_ADNC_CO
    , T.EXPNDTR_PRICE
    , T.EXPNDTR_PRICE / T.MOVIE_ADNC_CO AS SEAT_PER_EXPNDTR_PRICE
    , (SELECT METRP_AT FROM CTPRVN_INFO AS P WHERE T.CTPRVN_CD = P.CTPRVN_CD) AS METRP_AREA_AT
FROM colct_movie_ctrpvn_accto_stats AS T
WHERE T.BASE_DE = ?
UNION ALL
SELECT
    M.BASE_DE
    , '00' AS CTPRVN_CD
    , '전국' AS CTPRVN_NM
    , M.MOVIE_ADNC_CO
    , (M.MOVIE_ADNC_CO * 1000 / (
        IFNULL(
                (
                    SELECT
                        POPLTN_CO
                    FROM CTPRVN_ACCTO_POPLTN_INFO AS PP
                    WHERE PP.CTPRVN_CD = '00'
                        AND PP.BASE_YM = SUBSTR(M.BASE_DE, 1, 6)
                )
                , (
                SELECT
                    POPLTN_CO
                FROM CTPRVN_ACCTO_POPLTN_INFO AS PP
                WHERE PP.CTPRVN_CD = '00'
                    AND PP.BASE_YM = (
                        SELECT
                            MAX(BASE_YM) AS BASE_YM
                        FROM CTPRVN_ACCTO_POPLTN_INFO AS P
                        WHERE P.CTPRVN_CD = '00'
                    )
                )
        )
    )) AS POPULATION_PER_ADNC_CO
    , M.EXPNDTR_PRICE
    , M.MOVIE_ADNC_CO / M.EXPNDTR_PRICE AS SEAT_PER_EXPNDTR_PRICE
    , 'N' AS METRP_AREA_AT
FROM colct_movie_sales_stats AS M
WHERE M.BASE_DE = ?