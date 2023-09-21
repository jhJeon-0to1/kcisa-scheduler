package scheduler.kcisa.configuration.collection;

import org.quartz.*;
import org.quartz.impl.matchers.KeyMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.collection.mobile.MobileIndexJob;
import scheduler.kcisa.job.mart.mobile.MobileIndexListener;

import javax.annotation.PostConstruct;

@Configuration
public class MobileConfiguration {
    private final Scheduler scheduler;

    @Autowired
    protected MobileConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    protected void start() throws SchedulerException {
        JobDataMap mobileJobDataMap = new JobDataMap();
        mobileJobDataMap.put("retryCount", 0);
        mobileJobDataMap.put("maxRetryCount", 3);
        mobileJobDataMap.put("cronExpression", "0 0 2 * * ?");

        JobDetail mobileJobDetail = JobBuilder.newJob(MobileIndexJob.class).withIdentity("모바일 지수 추가", "모바일").usingJobData(mobileJobDataMap).build();

        Trigger mobileTrigger = TriggerBuilder.newTrigger().forJob(mobileJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).withRepeatCount(0)).build();


        MobileIndexListener mobileIndexListener = new MobileIndexListener();
        scheduler.getListenerManager().addJobListener(mobileIndexListener, KeyMatcher.keyEquals(mobileJobDetail.getKey()));

//        scheduler.scheduleJob(mobileJobDetail, mobileTrigger);
    }
}
