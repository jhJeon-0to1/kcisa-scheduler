insert into
    analysis_model.lsr_mvmn_qy_info
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD,
 CTPRVN_NM, DSTRCT_TY_CD, DSTRCT_TY_NM, MVMN_QY)
select
    BASE_DE
  , BASE_YEAR
  , BASE_MT
  , BASE_DAY
  , CTPRVN_CD
  , CTPRVN_NM
  , '01'     as DSTRCT_TY_CD
  , '생활 지역 ' as DSTRCT_TY_NM
  , MVMN_QY
from
    colct_lsr_mvmn_qy_info
where
    DSTRCT_TY_CD = '06'
union all
select
    BASE_DE
  , MAX(BASE_YEAR)
  , MAX(BASE_MT)
  , MAX(BASE_DAY)
  , CTPRVN_CD
  , MAX(CTPRVN_NM)
  , '02'    as DSTRCT_TY_CD
  , '소비 지역' as DSTRCT_TY_NM
  , SUM(MVMN_QY)
from
    colct_lsr_mvmn_qy_info
where
    DSTRCT_TY_CD in ('01', '03')
group by
    base_de, CTPRVN_CD
union all
select
    BASE_DE
  , MAX(BASE_YEAR)
  , MAX(BASE_MT)
  , MAX(BASE_DAY)
  , CTPRVN_CD
  , MAX(CTPRVN_NM)
  , '03'       as DSTRCT_TY_CD
  , '관광/레저 지역' as DSTRCT_TY_NM
  , SUM(MVMN_QY)
from
    colct_lsr_mvmn_qy_info
where
    DSTRCT_TY_CD in ('02', '05')
group by
    base_de, CTPRVN_CD
union all
select
    BASE_DE
  , MAX(BASE_YEAR)
  , MAX(BASE_MT)
  , MAX(BASE_DAY)
  , CTPRVN_CD
  , MAX(CTPRVN_NM)
  , '00'    as DSTRCT_TY_CD
  , '전체 지역' as DSTRCT_TY_NM
  , SUM(MVMN_QY)
from
    colct_lsr_mvmn_qy_info
where
        DSTRCT_TY_CD in ('02', '05', '01', '03', '06')
group by
    base_de, CTPRVN_CD
ON DUPLICATE KEY UPDATE
    MVMN_QY = VALUES(MVMN_QY)