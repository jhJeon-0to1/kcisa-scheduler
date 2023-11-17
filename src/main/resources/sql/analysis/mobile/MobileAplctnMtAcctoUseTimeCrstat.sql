INSERT INTO mobile_aplctn_mt_accto_use_time_crstat
(BASE_YM, BASE_YEAR, BASE_MT, CTGRY_NM, AVRG_USE_TIME)
select
	BASE_YM
	, SUBSTR(BASE_YM, 1,4) as BASE_YEAR
	, SUBSTR(BASE_YM, 5,2) as BASE_MT
	, '전체' as CTGRY_NM
	, SUM(AVRG_USE_TIME) as AVRG_USE_TIME
from (
	select
		SUBSTR(MIN(BASE_DE), 1, 6) as BASE_YM
		, UPPER_CTGRY_NM
		, SUM(ALL_USE_TIME / ALL_EMPR_CO) / 3600 as AVRG_USE_TIME
	from colct_mobile_ctgry_use_qy_info
	where UPPER_CTGRY_NM != '전체' and LWPRT_CTGRY_NM = ''
	    AND BASE_DE BETWEEN ? AND ?
	GROUP BY UPPER_CTGRY_NM
) as T1
group by BASE_YM
union all
select
	SUBSTR(MIN(BASE_DE), 1, 6) as BASE_YM
	, SUBSTR(MIN(BASE_DE), 1,4) as BASE_YEAR
	, SUBSTR(MIN(BASE_DE), 5,2) as BASE_MT
	, UPPER_CTGRY_NM
	, SUM(ALL_USE_TIME / ALL_EMPR_CO) / 3600 as AVRG_USE_TIME
from colct_mobile_ctgry_use_qy_info
where UPPER_CTGRY_NM != '전체' and LWPRT_CTGRY_NM = ''
    AND BASE_DE BETWEEN ? AND ?
group by UPPER_CTGRY_NM