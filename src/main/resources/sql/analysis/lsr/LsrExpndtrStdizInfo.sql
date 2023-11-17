insert into lsr_expndtr_stdiz_info
(BASE_DE,BASE_YEAR,BASE_MT,BASE_DAY,CTPRVN_CD,CTPRVN_NM,INDUTY_TY_CD,INDUTY_TY_NM,FLCTTN_RT)
select
	BASE_DE
	, MAX(BASE_YEAR)
	, MAX(BASE_MT)
	, MAX(BASE_DAY)
	, CTPRVN_CD
	, MAX(CTPRVN_NM)
	, '01' as INDUTY_TY_CD
	, '외식 지출' as INDUTY_TY_NM
	, FLCTTN_RT
from colct_lsr_expndtr_stdiz_info
where INDUTY_TY_CD = '06'
group by BASE_DE , CTPRVN_CD
union all
select
	BASE_DE
	, MAX(BASE_YEAR)
	, MAX(BASE_MT)
	, MAX(BASE_DAY)
	, CTPRVN_CD
	, MAX(CTPRVN_NM)
	, '02' as INDUTY_TY_CD
	, '숙박 지출' as INDUTY_TY_NM
	, FLCTTN_RT
from colct_lsr_expndtr_stdiz_info
where INDUTY_TY_CD = '07'
group by BASE_DE , CTPRVN_CD
union all
select
	BASE_DE
	, MAX(BASE_YEAR)
	, MAX(BASE_MT)
	, MAX(BASE_DAY)
	, CTPRVN_CD
	, MAX(CTPRVN_NM)
	, '03' as INDUTY_TY_CD
	, '레저 지출' as INDUTY_TY_NM
	, FLCTTN_RT
from colct_lsr_expndtr_stdiz_info
where INDUTY_TY_CD = '04'
group by BASE_DE , CTPRVN_CD
union all
select
	BASE_DE
	, MAX(BASE_YEAR)
	, MAX(BASE_MT)
	, MAX(BASE_DAY)
	, CTPRVN_CD
	, MAX(CTPRVN_NM)
	, '04' as INDUTY_TY_CD
	, '소비 지출' as INDUTY_TY_NM
	, FLCTTN_RT
from colct_lsr_expndtr_stdiz_info
where INDUTY_TY_CD = '02'
group by BASE_DE , CTPRVN_CD
ON DUPLICATE KEY UPDATE
    FLCTTN_RT = VALUES(FLCTTN_RT)
