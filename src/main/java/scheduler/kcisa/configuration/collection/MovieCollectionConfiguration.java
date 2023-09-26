package scheduler.kcisa.configuration.collection;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.collection.movie.*;

import javax.annotation.PostConstruct;

@Configuration
public class MovieCollectionConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public MovieCollectionConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    public void start() throws SchedulerException {
        JobDetail MtAcctoCtprvnAcctoStatsJobDetail = JobBuilder.newJob(MtAcctoCtprvnAcctoStatsJob.class).withIdentity("영화 월별 지역별 통계 수집", "영화 관람").build();
        Trigger MtAcctoCtprvnAcctoStatsTrigger = TriggerBuilder.newTrigger().forJob(MtAcctoCtprvnAcctoStatsJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 2 * ?")) // 매월 2일 2시에 실행
                .build();
//        scheduler.scheduleJob(MtAcctoCtprvnAcctoStatsJobDetail, MtAcctoCtprvnAcctoStatsTrigger); // 영화 월별 지역별 통계 수집

        JobDetail CtprvnAcctoStatsJobDetail = JobBuilder.newJob(CtrpvnAcctoStatsJob.class).withIdentity("영화 지역별 통계 수집", "영화 관람").build();
        Trigger CtprvnAcctoStatsTrigger = TriggerBuilder.newTrigger().forJob(CtprvnAcctoStatsJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 * * ?")) // 매일 2시에 실행
                .build();
//        scheduler.scheduleJob(CtprvnAcctoStatsJobDetail, CtprvnAcctoStatsTrigger); // 영화 지역별 통계 수집

        JobDetail SalesStatsJobDetail = JobBuilder.newJob(SalesStatsJob.class).withIdentity("영화 일별 매출액 수집", "영화 관람").build();
        Trigger SalesStatsTrigger = TriggerBuilder.newTrigger().forJob(SalesStatsJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 * * ?")) // 매일 2시에 실행
                .build();
//        scheduler.scheduleJob(SalesStatsJobDetail, SalesStatsTrigger); // 영화 일별 매출액 수집


        JobDetail MtAcctoSalesStatsJobDetail = JobBuilder.newJob(MtAcctoSalesStatsJob.class).withIdentity("영화 월별 매출액 수집", "영화 관람").build();
        Trigger MtAcctoSalesStatsTrigger = TriggerBuilder.newTrigger().forJob(MtAcctoSalesStatsJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 2 * ?")) // 매월 2일 2시에 실행
                .build();
//        scheduler.scheduleJob(MtAcctoSalesStatsJobDetail, MtAcctoSalesStatsTrigger); // 영화 월별 매출액 수집

//        6개월 전것 삭제하는 로직 추가해야함
        JobDetail InfoJobDetail = JobBuilder.newJob(InfoJob.class).withIdentity("영화 정보 수집", "영화 관람").build();
        Trigger InfoTrigger = TriggerBuilder.newTrigger().forJob(InfoJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 2 * ?")) // 매월 2일 2시에 실행
                .build();
        scheduler.scheduleJob(InfoJobDetail, InfoTrigger); // 영화 정보 수집
    }
}
