package scheduler.kcisa.configuration.collection;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.collection.sports.MatchInfoJob;
import scheduler.kcisa.job.collection.sports.ViewingInfoJob;

import javax.annotation.PostConstruct;

@Configuration
public class SportsCollectionConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public SportsCollectionConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    protected void start() throws SchedulerException {
        JobDetail sportsRegionJobDetail = JobBuilder.newJob(ViewingInfoJob.class)
                .withIdentity("스포츠 관람 정보 수집", "스포츠 관람").build();
        JobDetail sportsDailyJobDetail = JobBuilder.newJob(MatchInfoJob.class)
                .withIdentity("스포츠 경기 정보 수집", "스포츠 관람")
                .build();

        Trigger sportsRegionTrigger = TriggerBuilder.newTrigger().forJob(sportsRegionJobDetail)
                .withSchedule(
                        SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                                .withRepeatCount(0)
                        // 매일 새벽 2시에 실행
                        // CronScheduleBuilder.cronSchedule("0 0 2 * * ?")
                        // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul"))
                )
                .build();
        Trigger sportsDailyTrigger = TriggerBuilder.newTrigger().forJob(sportsDailyJobDetail)
                .withSchedule(
                        SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                                .withRepeatCount(0)
                        // 매일 새벽 2시에 실행
                        // CronScheduleBuilder.cronSchedule("0 0 2 * * ?")
                        // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul"))
                )
                .build();

        // scheduler.scheduleJob(sportsRegionJobDetail, sportsRegionTrigger);
        // scheduler.scheduleJob(sportsDailyJobDetail, sportsDailyTrigger);
    }
}
