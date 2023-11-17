package scheduler.kcisa.configuration.collection;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.collection.pblprfr.*;

import javax.annotation.PostConstruct;

@Configuration
public class PblprfrCollectionConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public PblprfrCollectionConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    public void start() throws SchedulerException {
        JobDetail PlbprfrViewngCtprvnAcctoStatJobDetail = JobBuilder.newJob(PblprfrViewngCtprvnAcctoStat.class)
                .withIdentity("공연 관람 시도별 통계 수집", "공연 관람").build();
        Trigger PlbprfrViewngCtprvnAcctoStatTrigger = TriggerBuilder.newTrigger().forJob(PlbprfrViewngCtprvnAcctoStatJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 * * ?")
//                                .inTimeZone(TimeZone.getTimeZone("Asia/Seoul"))) // 매일 2시에 실행
                .build();
//        scheduler.scheduleJob(PlbprfrViewngCtprvnAcctoStatJobDetail, PlbprfrViewngCtprvnAcctoStatTrigger);

        JobDetail PblprfrViewngMtAcctoCtprvnAcctoStatJobDetail = JobBuilder.newJob(PblprfrViewngMtAcctoCtprvnAcctoStat.class)
                .withIdentity("공연 관람 월별 시도별 통계 수집", "공연 관람").build();
        Trigger PblprfrViewngMtAcctoCtprvnAcctoStatTrigger = TriggerBuilder.newTrigger().forJob(PblprfrViewngMtAcctoCtprvnAcctoStatJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 2 * ?")
//                        .inTimeZone(TimeZone.getTimeZone("Asia/Seoul"))) // 매월 2일 2시에 실행
                .build();
//        scheduler.scheduleJob(PblprfrViewngMtAcctoCtprvnAcctoStatJobDetail, PblprfrViewngMtAcctoCtprvnAcctoStatTrigger);

        JobDetail PblprfrViewngYearAcctoCtprvnAcctoStatJobDetail = JobBuilder.newJob(PblprfrViewngYearAcctoCtprvnAcctoStat.class)
                .withIdentity("공연 관람 연도별 시도별 통계 수집", "공연 관람").build();
        Trigger PblprfrViewngYearAcctoCtprvnAcctoStatTrigger = TriggerBuilder.newTrigger().forJob(PblprfrViewngYearAcctoCtprvnAcctoStatJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 10 1 ?")
//                        .inTimeZone(TimeZone.getTimeZone("Asia/Seoul"))) // 매년 1월 10일 2시에 실행
                .build();
//        scheduler.scheduleJob(PblprfrViewngYearAcctoCtprvnAcctoStatJobDetail, PblprfrViewngYearAcctoCtprvnAcctoStatTrigger);


        JobDetail PblprfrFcltyInfoJobDetail = JobBuilder.newJob(PblprfrFcltyInfo.class)
                .withIdentity("공연 시설 정보 수집", "공연 관람")
                .build();
        Trigger PblprfrFcltyInfoTrigger = TriggerBuilder.newTrigger().forJob(PblprfrFcltyInfoJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 2 * ?") // 매월 2일 2시에 실행
                //                 .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
//        scheduler.scheduleJob(PblprfrFcltyInfoJobDetail, PblprfrFcltyInfoTrigger);


        JobDetail PblprfrFcltyDetailInfoJobDetail = JobBuilder.newJob(PblprfrFcltyDetailInfo.class)
                .withIdentity("공연 시설 상세 정보 수집", "공연 관람").build();
        Trigger PblprfrFcltyDetailInfoTrigger = TriggerBuilder.newTrigger().forJob(PblprfrFcltyDetailInfoJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 30 2-7 2 * ?") // 매월 2일 2시부터 7시까지 30분마다 이미 있으면 바로 종료
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
//        scheduler.scheduleJob(PblprfrFcltyDetailInfoJobDetail, PblprfrFcltyDetailInfoTrigger);
    }
}
