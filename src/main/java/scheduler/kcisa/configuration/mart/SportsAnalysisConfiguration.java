package scheduler.kcisa.configuration.mart;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.mart.sports.*;

import javax.annotation.PostConstruct;

@Configuration
public class SportsAnalysisConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public SportsAnalysisConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    public void start() throws SchedulerException {
        JobDetail sportsActivateJobDetail = JobBuilder.newJob(SportsActivateCrstatsJob.class)
                .withIdentity("스포츠 활성화 현황 분석", "스포츠 관람").build();
        JobDetail sportsViewingStateJobDetail = JobBuilder.newJob(SportsViewngCrstatJob.class)
                .withIdentity("스포츠 관람 현황 분석", "스포츠 관람").build();
        JobDetail monthSportsViewingJobDetail = JobBuilder.newJob(MtAcctoViewngCrstatJob.class)
                .withIdentity("월별 스포츠 관람 현황 분석", "스포츠 관람").build();
        JobDetail sportsMatchStateJobDetail = JobBuilder.newJob(SportsMatchCrstatJob.class)
                .withIdentity("스포츠 경기 현황 분석", "스포츠 관람").build();
        JobDetail sportsMatchInfoJobDetail = JobBuilder.newJob(SportsMatchInfoJob.class)
                .withIdentity("스포츠 경기 정보 분석", "스포츠 관람").build();

        Trigger sportsActivateTrigger = TriggerBuilder.newTrigger().forJob(sportsActivateJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 2 * ?") // 매월 2일 3시부터
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        Trigger sportsViewingStateTrigger = TriggerBuilder.newTrigger().forJob(sportsViewingStateJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 * * ?") // 매일 3시부터
                // 7시까지 0초마다 이미 있으면 바로 종료
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        Trigger monthSportsViewingTrigger = TriggerBuilder.newTrigger().forJob(monthSportsViewingJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 2 * ?") // 매월 2일 3시부터
                // 7시까지 0초마다 이미 있으면 바로 종료
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        Trigger sportsMatchStateTrigger = TriggerBuilder.newTrigger().forJob(sportsMatchStateJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 * * ?") // 매일 3시부터
                // 7시까지 0초마다 이미 있으면 바로 종료
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        Trigger sportsMatchInfoTrigger = TriggerBuilder.newTrigger().forJob(sportsMatchInfoJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 * * ?") // 매일 3시부터
                // 7시까지 0초마다 이미 있으면 바로 종료
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();

        // scheduler.scheduleJob(sportsActivateJobDetail, sportsActivateTrigger);
        // scheduler.scheduleJob(sportsViewingStateJobDetail,
        // sportsViewingStateTrigger);
        // scheduler.scheduleJob(monthSportsViewingJobDetail,
        // monthSportsViewingTrigger);
        // scheduler.scheduleJob(sportsMatchStateJobDetail, sportsMatchStateTrigger);
        // scheduler.scheduleJob(sportsMatchInfoJobDetail, sportsMatchInfoTrigger);
    }
}
