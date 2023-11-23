package scheduler.kcisa.configuration.collection;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.collection.movie.CtrpvnAcctoStatsJob;
import scheduler.kcisa.job.collection.movie.InfoJob;
import scheduler.kcisa.job.collection.movie.SalesStatsJob;
import scheduler.kcisa.job.collection.movie.monthly.MtAcctoCtprvnAcctoStatsJob;
import scheduler.kcisa.job.collection.movie.monthly.MtAcctoSalesStatsJob;
import scheduler.kcisa.job.collection.movie.yearly.YearAcctoCtprvnAcctoStatsJob;
import scheduler.kcisa.job.collection.movie.yearly.YearAcctoSalesStatsJob;

import javax.annotation.PostConstruct;
import java.util.TimeZone;

@Configuration
public class MovieCollectionConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public MovieCollectionConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void dailyCollection() throws SchedulerException {
        JobDetail CtprvnAcctoStatsJobDetail = JobBuilder.newJob(CtrpvnAcctoStatsJob.class).withIdentity("영화 시도별 통계 수집", "영화 관람").build();
        Trigger CtprvnAcctoStatsTrigger = TriggerBuilder.newTrigger().forJob(CtprvnAcctoStatsJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
//                매일 2시에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 * * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(CtprvnAcctoStatsJobDetail, CtprvnAcctoStatsTrigger); // 영화 지역별 통계 수집

        JobDetail SalesStatsJobDetail = JobBuilder.newJob(SalesStatsJob.class).withIdentity("영화 매출액 통계 수집", "영화 관람").build();
        Trigger SalesStatsTrigger = TriggerBuilder.newTrigger().forJob(SalesStatsJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
//                매일 2시에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 * * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(SalesStatsJobDetail, SalesStatsTrigger); // 영화 일별 매출액 수집
    }

    public void monthlyCollection() throws SchedulerException {
        JobDetail MtAcctoCtprvnAcctoStatsJobDetail = JobBuilder.newJob(MtAcctoCtprvnAcctoStatsJob.class).withIdentity("영화 월별 시도별 통계 수집", "영화 관람").build();
        Trigger MtAcctoCtprvnAcctoStatsTrigger = TriggerBuilder.newTrigger().forJob(MtAcctoCtprvnAcctoStatsJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매월 5일 2시에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 5 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(MtAcctoCtprvnAcctoStatsJobDetail, MtAcctoCtprvnAcctoStatsTrigger); // 영화 월별 지역별 통계 수집

        JobDetail MtAcctoSalesStatsJobDetail = JobBuilder.newJob(MtAcctoSalesStatsJob.class).withIdentity("영화 월별 매출액 통계 수집", "영화 관람").build();
        Trigger MtAcctoSalesStatsTrigger = TriggerBuilder.newTrigger().forJob(MtAcctoSalesStatsJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매월 5일 2시에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 5 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(MtAcctoSalesStatsJobDetail, MtAcctoSalesStatsTrigger); // 영화 월별 매출액 수집

        JobDetail InfoJobDetail = JobBuilder.newJob(InfoJob.class).withIdentity("영화 정보 수집", "영화 관람").build();
        Trigger InfoTrigger = TriggerBuilder.newTrigger().forJob(InfoJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매월 5일 2시에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 5 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(InfoJobDetail, InfoTrigger); // 영화 정보 수집
    }

    public void yearlyCollection() throws SchedulerException {
        JobDetail YearAcctoCtprvnAcctoStatsJobDetail = JobBuilder.newJob(YearAcctoCtprvnAcctoStatsJob.class).withIdentity("영화 연도별 시도별 통계 수집", "영화 관람").build();
        Trigger YearAcctoCtprvnAcctoStatsTrigger = TriggerBuilder.newTrigger().forJob(YearAcctoCtprvnAcctoStatsJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매년 1월 5일 2시에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 5 1 ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(YearAcctoCtprvnAcctoStatsJobDetail, YearAcctoCtprvnAcctoStatsTrigger); // 영화 연도별 지역별 통계 수집

        JobDetail YearAcctoSalesStatsJobDetail = JobBuilder.newJob(YearAcctoSalesStatsJob.class).withIdentity("영화 연도별 매출액 통계 수집", "영화 관람").build();
        Trigger YearAcctoSalesStatsTrigger = TriggerBuilder.newTrigger().forJob(YearAcctoSalesStatsJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매년 1월 5일 2시에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 5 1 ?"))
                .build();
        scheduler.scheduleJob(YearAcctoSalesStatsJobDetail, YearAcctoSalesStatsTrigger); // 영화 연도별 매출액 수집
    }

    @PostConstruct
    public void start() throws SchedulerException {
        dailyCollection();
        monthlyCollection();
        yearlyCollection();
    }
}
