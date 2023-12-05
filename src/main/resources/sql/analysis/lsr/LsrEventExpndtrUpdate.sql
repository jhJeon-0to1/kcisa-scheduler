update analysis_model.lsr_event_info as a
set
	a.EXPNDTR_BASE_DE = (select distinct b.BASE_DE
	                     from analysis_model.lsr_expndtr_stdiz_info as b
	                     where
		                     date(a.base_de) between date(b.base_de) and date_add(
				                     b.BASE_DE, interval 6
				                     day))
where
	a.EXPNDTR_BASE_DE is null;
