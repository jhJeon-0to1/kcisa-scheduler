insert into mobile_aplctn_use_time_crstat
(BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, WKDAY_NM, CTGRY_NM, AVRG_USE_TIME)
select
	BASE_DE
	, SUBSTR(BASE_DE, 1,4) as BASE_YEAR
	, SUBSTR(BASE_DE, 5,2) as BASE_MT
	, SUBSTR(BASE_DE, 7,2) as BASE_DAY
	, MAX(WKDAY) as WKDAY
	, '전체' as UPPER_CTGRY_NM
	, SUM(AVRG_USE_TIME) as AVRG_USE_TIME
from (
	select
		BASE_DE
		, UPPER_CTGRY_NM
		, ALL_USE_TIME / ALL_EMPR_CO / 3600 as AVRG_USE_TIME
		, case weekday(BASE_DE)
			when 0 then '월'
			when 1 then '화'
			when 2 then '수'
			when 3 then '목'
			when 4 then '금'
			when 5 then '토'
			when 6 then '일'
			end as WKDAY
	from colct_mobile_ctgry_use_qy_info
	where UPPER_CTGRY_NM != '전체' and LWPRT_CTGRY_NM = ''
	    AND BASE_DE = ?
) as T1
group by BASE_DE
union all
select
	BASE_DE
	, BASE_YEAR
	, BASE_MT
	, BASE_DAY
	, (case weekday(BASE_DE)
		when 0 then '월'
		when 1 then '화'
		when 2 then '수'
		when 3 then '목'
		when 4 then '금'
		when 5 then '토'
		when 6 then '일'
		end) as WKDAY
	, UPPER_CTGRY_NM
	, ALL_USE_TIME / ALL_EMPR_CO / 3600 as AVRG_USE_TIME
from colct_mobile_ctgry_use_qy_info
where UPPER_CTGRY_NM != '전체' and LWPRT_CTGRY_NM = ''
    AND BASE_DE = ?