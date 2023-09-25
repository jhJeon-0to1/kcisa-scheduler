package scheduler.kcisa.configuration.mart;

import javax.annotation.PostConstruct;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import scheduler.kcisa.job.mart.sports.MonthSportsViewingJob;
import scheduler.kcisa.job.mart.sports.SportsActivateJob;
import scheduler.kcisa.job.mart.sports.SportsMatchInfoJob;
import scheduler.kcisa.job.mart.sports.SportsMatchJob;
import scheduler.kcisa.job.mart.sports.SportsViewingJob;

@Configuration
public class SportsMartConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public SportsMartConfiguration(Scheduler scheduler) {
            this.scheduler = scheduler;
    }

    @PostConstruct
    public void start() throws SchedulerException {
            JobDetail sportsActivateJobDetail = JobBuilder.newJob(SportsActivateJob.class)
                            .withIdentity("스포츠 활성화 현황 분석", "스포츠 관람").build();
            JobDetail sportsViewingJobDetail = JobBuilder.newJob(SportsViewingJob.class)
                            .withIdentity("스포츠 관람 현황 분석", "스포츠 관람").build();
            JobDetail monthSportsViewingJobDetail = JobBuilder.newJob(MonthSportsViewingJob.class).withIdentity("월별 스포츠 관람 현황 분석", "스포츠 관람").build();
            JobDetail sportsMatchJobDetail = JobBuilder.newJob(SportsMatchJob.class).withIdentity("스포츠 경기 현황 분석", "스포츠 관람").build();
            JobDetail sportsMatchInfoJobDetail = JobBuilder.newJob(SportsMatchInfoJob.class).withIdentity("스포츠 경기 정보 분석", "스포츠 관람").build();

            Trigger sportsActivateTrigger = TriggerBuilder.newTrigger().forJob(sportsActivateJobDetail)
                            .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                                            .withRepeatCount(0))
                            // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 2 * ?")
                            // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                            .build();
            Trigger sportsViewingTrigger = TriggerBuilder.newTrigger().forJob(sportsViewingJobDetail)
                        .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                                        .withRepeatCount(0))
                        // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 * * ?") // 매일 3시부터 7시까지 0초마다 이미 있으면 바로 종료
                        // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                        .build();
            Trigger monthSportsViewingTrigger = TriggerBuilder.newTrigger().forJob(monthSportsViewingJobDetail)
                        .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                                        .withRepeatCount(0))
                        // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 2 * ?") // 매월 2일 3시부터 7시까지 0초마다 이미 있으면 바로 종료
                        // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                        .build();
            Trigger sportsMatchTrigger = TriggerBuilder.newTrigger().forJob(sportsMatchJobDetail)
                        .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                                        .withRepeatCount(0))
                        // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 * * ?") // 매일 3시부터 7시까지 0초마다 이미 있으면 바로 종료
                        // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                        .build();
            Trigger sportsMatchInfoTrigger = TriggerBuilder.newTrigger().forJob(sportsMatchInfoJobDetail)
                        .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                                        .withRepeatCount(0))
                        // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 * * ?") // 매일 3시부터 7시까지 0초마다 이미 있으면 바로 종료
                        // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                        .build();

            // scheduler.scheduleJob(sportsActivateJobDetail,sportsActivateTrigger);
            // scheduler.scheduleJob(sportsViewingJobDetail, sportsViewingTrigger);
            // scheduler.scheduleJob(monthSportsViewingJobDetail, monthSportsViewingTrigger);
            // scheduler.scheduleJob(sportsMatchJobDetail, sportsMatchTrigger);
            // scheduler.scheduleJob(sportsMatchInfoJobDetail, sportsMatchInfoTrigger);
    }
}
