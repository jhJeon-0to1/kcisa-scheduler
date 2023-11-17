INSERT INTO mobile_entmnt_aplctn_mt_accto_use_time_crstat
(BASE_YM, BASE_YEAR, BASE_MT, ENTMNT_CTGRY_NM, AVRG_USE_TIME)
select
	SUBSTR(MIN(BASE_DE), 1, 6) as BASE_YM
	, SUBSTR(MIN(BASE_DE), 1, 4) as BASE_YEAR
	, SUBSTR(MIN(BASE_DE), 5, 2) as BASE_MT
	, (case LWPRT_CTGRY_NM when '' then '전체' else LWPRT_CTGRY_NM end) as ENTMNT_CTGRY_NM
	, SUM(ALL_USE_TIME / ALL_EMPR_CO) / 3600 as AVRG_USE_TIME
from colct_mobile_ctgry_use_qy_info
where UPPER_CTGRY_NM ='엔터테인먼트'
	and BASE_DE between ? and ?
group by LWPRT_CTGRY_NM