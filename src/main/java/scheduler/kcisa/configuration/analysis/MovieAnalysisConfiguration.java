package scheduler.kcisa.configuration.analysis;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.analysis.movie.MovieActivateCrstatJob;

import javax.annotation.PostConstruct;
import java.util.TimeZone;

@Configuration
public class MovieAnalysisConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public MovieAnalysisConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    public void start() throws SchedulerException {
        JobDetail movieActivateCrstatJobDetail = JobBuilder.newJob(MovieActivateCrstatJob.class).withIdentity("영화 활성화 현황 분석", "영화 관람").build();
        Trigger movieActivateCrstatTrigger = TriggerBuilder.newTrigger().forJob(movieActivateCrstatJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 2 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul"))) // 매월 2일 2시부터
                .build();
//        scheduler.scheduleJob(movieActivateCrstatJobDetail, movieActivateCrstatTrigger);
    }
}
