package scheduler.kcisa.configuration.collection;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.collection.kobis.KobisDailyJob;
import scheduler.kcisa.job.collection.kobis.KobisMovieJob;
import scheduler.kcisa.job.collection.kobis.KobisRegionJob;

import javax.annotation.PostConstruct;

@Configuration
public class KobisConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public KobisConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    public void start() throws SchedulerException {
        JobDetail kobisRegionJobDetail = JobBuilder.newJob(KobisRegionJob.class).withIdentity("영화 지역별 일별", "TEST").build();
        JobDetail kobisDailyJobDetail = JobBuilder.newJob(KobisDailyJob.class).withIdentity("영화 일별 매출액", "TEST").build();
        JobDetail kobisMovieJobDetail = JobBuilder.newJob(KobisMovieJob.class).withIdentity("영화 정보", "TEST").build();


        Trigger kobisRegionTrigger = TriggerBuilder.newTrigger().forJob(kobisRegionJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0)).build();
        Trigger kobisDailyTrigger = TriggerBuilder.newTrigger().forJob(kobisDailyJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0)).build();
        Trigger kobisMovieTrigger = TriggerBuilder.newTrigger().forJob(kobisMovieJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0)).build();


//        scheduler.scheduleJob(kobisRegionJobDetail, kobisRegionTrigger);
//        scheduler.scheduleJob(kobisDailyJobDetail, kobisDailyTrigger);
//        scheduler.scheduleJob(kobisMovieJobDetail, kobisMovieTrigger);
    }
}
