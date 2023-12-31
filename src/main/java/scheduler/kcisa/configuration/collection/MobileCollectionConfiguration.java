package scheduler.kcisa.configuration.collection;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.collection.mobile.MobileCtgryUseQyInfo;

import javax.annotation.PostConstruct;
import java.util.TimeZone;

@Configuration
public class MobileCollectionConfiguration {
    private final Scheduler scheduler;

    @Autowired
    protected MobileCollectionConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void jobRun() throws SchedulerException {
        JobDetail mobileJobDetail = JobBuilder.newJob(MobileCtgryUseQyInfo.class)
                .withIdentity("모바일 카테고리 이용량 정보 수집", "모바일 이용량")
                .build();
        Trigger mobileTrigger = TriggerBuilder.newTrigger().forJob(mobileJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).withRepeatCount(0))
                // 매일 새벽 2시에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule(("0 0 2 * * ?")).inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(mobileJobDetail, mobileTrigger);
    }

    @PostConstruct
    protected void start() throws SchedulerException {
        jobRun();
    }
}
