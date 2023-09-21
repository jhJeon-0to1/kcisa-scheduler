package scheduler.kcisa.configuration.mart;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.mart.mobile.MobileIndexMart;

import javax.annotation.PostConstruct;

@Configuration
public class MartMobileConfiguration {
    private final Scheduler scheduler;

    @Autowired
    protected MartMobileConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    protected void start() throws SchedulerException {
        JobDetail mobileJobDetail = JobBuilder.newJob(MobileIndexMart.class).withIdentity("모바일 지수 통계", "모바일 관람").build();


        Trigger mobileTrigger = TriggerBuilder.newTrigger().forJob(mobileJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).withRepeatCount(0)).build();


//        scheduler.scheduleJob(mobileJobDetail, mobileTrigger);
    }
}
