package scheduler.kcisa.configuration.analysis;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.analysis.movie.MovieActivateCrstatJob;
import scheduler.kcisa.job.analysis.movie.MovieRlsCrstatJob;
import scheduler.kcisa.job.analysis.movie.MovieViewngCrstatJob;
import scheduler.kcisa.job.analysis.movie.monthly.MtAcctoMovieRlsCrstat;
import scheduler.kcisa.job.analysis.movie.monthly.MtAcctoMovieViewngCrstatJob;
import scheduler.kcisa.job.analysis.movie.yearly.MovieYearAcctoRlsCrstat;
import scheduler.kcisa.job.analysis.movie.yearly.MovieYearAcctoViewngCrstatJob;

import javax.annotation.PostConstruct;
import java.util.TimeZone;

@Configuration
public class MovieAnalysisConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public MovieAnalysisConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void daily() throws SchedulerException {
        JobDetail movieViewingCrstatJobDetail = JobBuilder.newJob(MovieViewngCrstatJob.class).withIdentity("영화 관람 현황 분석", "영화 관람").build();
        Trigger movieViewingCrstatTrigger = TriggerBuilder.newTrigger().forJob(movieViewingCrstatJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매일 3시부터 7시까지 매 0분마다 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 * * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(movieViewingCrstatJobDetail, movieViewingCrstatTrigger);

        JobDetail movieRlsCrstatJobDetail = JobBuilder.newJob(MovieRlsCrstatJob.class).withIdentity("영화 개봉 현황 분석", "영화 관람").build();
        Trigger movieRlsCrstatTrigger = TriggerBuilder.newTrigger().forJob(movieRlsCrstatJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매일 3시부터 7시까지 매 0분마다 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 * * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(movieRlsCrstatJobDetail, movieRlsCrstatTrigger);
    }

    public void monthly() throws SchedulerException {
        JobDetail movieActivateCrstatJobDetail = JobBuilder.newJob(MovieActivateCrstatJob.class).withIdentity("영화 활성화 현황 분석", "영화 관람").build();
        Trigger movieActivateCrstatTrigger = TriggerBuilder.newTrigger().forJob(movieActivateCrstatJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매월 10일 3시부터 7시까지 매 0분마다 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 10 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(movieActivateCrstatJobDetail, movieActivateCrstatTrigger);


        JobDetail MtAcctoMovieViewngCrstatJobDetail = JobBuilder.newJob(MtAcctoMovieViewngCrstatJob.class).withIdentity("월별 영화 관람 현황 분석", "영화 관람").build();
        Trigger MtAcctoMovieViewngCrstatTrigger = TriggerBuilder.newTrigger().forJob(MtAcctoMovieViewngCrstatJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매월 10일 3시부터 7시까지 매 0분마다 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 10 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(MtAcctoMovieViewngCrstatJobDetail, MtAcctoMovieViewngCrstatTrigger);

        JobDetail MtAcctoMovieRlsCrstatJobDetail = JobBuilder.newJob(MtAcctoMovieRlsCrstat.class).withIdentity("월별 영화 개봉 현황 분석", "영화 관람").build();
        Trigger MtAcctoMovieRlsCrstatTrigger = TriggerBuilder.newTrigger().forJob(MtAcctoMovieRlsCrstatJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매월 10일 3시부터 7시까지 매 0분마다 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 10 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(MtAcctoMovieRlsCrstatJobDetail, MtAcctoMovieRlsCrstatTrigger);
    }

    public void yearly() throws SchedulerException {
        JobDetail MovieYearAcctoRlsCrstatJobDetail = JobBuilder.newJob(MovieYearAcctoRlsCrstat.class).withIdentity("영화 연도별 개막 현황 분석", "영화 관람").build();
        Trigger MovieYearAcctoRlsCrstatTrigger = TriggerBuilder.newTrigger().forJob(MovieYearAcctoRlsCrstatJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매년 1월 10일 3시부터 7시까지 매 0분마다 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 10 1 ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(MovieYearAcctoRlsCrstatJobDetail, MovieYearAcctoRlsCrstatTrigger);

        JobDetail MovieYearAcctoViewngCrstatJobDetail = JobBuilder.newJob(MovieYearAcctoViewngCrstatJob.class).withIdentity("영화 연도별 관람 현황 분석", "영화 관람").build();
        Trigger MovieYearAcctoViewngCrstatTrigger = TriggerBuilder.newTrigger().forJob(MovieYearAcctoViewngCrstatJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매년 1월 10일 3시부터 7시까지 매 0분마다 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 10 1 ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(MovieYearAcctoViewngCrstatJobDetail, MovieYearAcctoViewngCrstatTrigger);
    }

    @PostConstruct
    public void start() throws SchedulerException {
        daily();
        monthly();
        yearly();
    }
}



