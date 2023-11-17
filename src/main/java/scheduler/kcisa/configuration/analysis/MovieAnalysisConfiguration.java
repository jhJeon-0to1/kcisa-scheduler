package scheduler.kcisa.configuration.analysis;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.analysis.movie.MovieActivateCrstatJob;
import scheduler.kcisa.job.analysis.movie.MovieRlsCrstatJob;
import scheduler.kcisa.job.analysis.movie.MovieViewngCrstatJob;
import scheduler.kcisa.job.analysis.movie.monthly.MtAcctoMovieRlsCrstat;
import scheduler.kcisa.job.analysis.movie.monthly.MtAcctoMovieViewngCrstatJob;

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
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 2 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul"))) // 매월 2일 3시부터 7시까지 0초마다 이미 있으면 바로 종료
                .build();
//        scheduler.scheduleJob(movieActivateCrstatJobDetail, movieActivateCrstatTrigger);

        JobDetail movieViewingCrstatJobDetail = JobBuilder.newJob(MovieViewngCrstatJob.class).withIdentity("영화 관람 현황 분석", "영화 관람").build();
        Trigger movieViewingCrstatTrigger = TriggerBuilder.newTrigger().forJob(movieViewingCrstatJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 * * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul"))) // 매일 3시부터 7시까지 0초마다 이미 있으면 바로 종료
                .build();
//        scheduler.scheduleJob(movieViewingCrstatJobDetail, movieViewingCrstatTrigger);

        JobDetail movieRlsCrstatJobDetail = JobBuilder.newJob(MovieRlsCrstatJob.class).withIdentity("영화 개봉 현황 분석", "영화 관람").build();
        Trigger movieRlsCrstatTrigger = TriggerBuilder.newTrigger().forJob(movieRlsCrstatJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 * * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul"))) // 매일 3시부터 7시까지 0초마다 이미 있으면 바로 종료
                .build();
//        scheduler.scheduleJob(movieRlsCrstatJobDetail, movieRlsCrstatTrigger);


        JobDetail MtAcctoMovieViewngCrstatJobDetail = JobBuilder.newJob(MtAcctoMovieViewngCrstatJob.class).withIdentity("월별 영화 관람 현황 분석", "영화 관람").build();
        Trigger MtAcctoMovieViewngCrstatTrigger = TriggerBuilder.newTrigger().forJob(MtAcctoMovieViewngCrstatJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 2 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul"))) // 매월 2일 3시부터 7시까지 0초마다 이미 있으면 바로 종료
                .build();
//        scheduler.scheduleJob(MtAcctoMovieViewngCrstatJobDetail, MtAcctoMovieViewngCrstatTrigger);

        JobDetail MtAcctoMovieRlsCrstatJobDetail = JobBuilder.newJob(MtAcctoMovieRlsCrstat.class).withIdentity("월별 영화 개봉 현황 분석", "영화 관람").build();
        Trigger MtAcctoMovieRlsCrstatTrigger = TriggerBuilder.newTrigger().forJob(MtAcctoMovieRlsCrstatJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 2 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul"))) // 매월 2일 3시부터 7시까지 0초마다 이미 있으면 바로 종료
                .build();
//        scheduler.scheduleJob(MtAcctoMovieRlsCrstatJobDetail, MtAcctoMovieRlsCrstatTrigger);
    }
}
