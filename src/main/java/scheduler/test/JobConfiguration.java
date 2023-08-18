package scheduler.test;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.test.job.common.CTPRVNJob;
import scheduler.test.job.mobile.MobileIndexJob;

import javax.annotation.PostConstruct;

@Configuration
public class JobConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public JobConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    public void start() throws SchedulerException {
        JobDetail jobDetail = JobBuilder.newJob(CTPRVNJob.class).withIdentity("시도 인구 추가", "TEST").build();
        JobDetail mobileJobDetail = JobBuilder.newJob(MobileIndexJob.class).withIdentity("모바일 지수 추가", "TEST").build();

        Trigger trigger = TriggerBuilder.newTrigger().forJob(jobDetail)
//                10초마다
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).repeatForever())
//                매달 1-10일 13시에 (매월 첫 주말, 공휴일 제외한 평일 12시 이후에 업데이트 되므로)
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 13 1-10 * ?").inTimeZone(java.util.TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        Trigger mobileTrigger = TriggerBuilder.newTrigger().forJob(mobileJobDetail)
//                10초마다
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).repeatForever()).build();

//        scheduler.scheduleJob(jobDetail, trigger);
        scheduler.scheduleJob(mobileJobDetail, mobileTrigger);
    }
}
