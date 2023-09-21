package scheduler.kcisa.configuration.collection;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.collection.sports.SportsDailyJob;
import scheduler.kcisa.job.collection.sports.SportsRegionJob;

import javax.annotation.PostConstruct;

@Configuration
public class SportsConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public SportsConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    protected void start() throws SchedulerException {
        JobDetail sportsRegionJobDetail = JobBuilder.newJob(SportsRegionJob.class).withIdentity("스포츠 지역별 관중 추가", "스포츠 관람").build();
        JobDetail sportsDailyJobDetail = JobBuilder.newJob(SportsDailyJob.class).withIdentity("스포츠 일별 경기 추가", "스포츠 관람").build();


        Trigger sportsRegionTrigger = TriggerBuilder.newTrigger().forJob(sportsRegionJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0)).build();
        Trigger sportsDailyTrigger = TriggerBuilder.newTrigger().forJob(sportsDailyJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0)).build();


//        scheduler.scheduleJob(sportsRegionJobDetail, sportsRegionTrigger);
//        scheduler.scheduleJob(sportsDailyJobDetail, sportsDailyTrigger);
    }
}
