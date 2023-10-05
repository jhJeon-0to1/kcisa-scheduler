package scheduler.kcisa.configuration.collection;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.collection.pblprfr.PblprfrFcltyDetailInfo;
import scheduler.kcisa.job.collection.pblprfr.PblprfrFcltyInfo;
import scheduler.kcisa.job.collection.pblprfr.PlbprfrViewngCtprvnAcctoStat;

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
        JobDetail kopisRegionJobDetail = JobBuilder.newJob(PlbprfrViewngCtprvnAcctoStat.class)
                .withIdentity("공연 지역별 관중 추가", "TEST").build();
        JobDetail PblprfrFcltyInfoJobDetail = JobBuilder.newJob(PblprfrFcltyInfo.class)
                .withIdentity("공연 시설 정보 수집", "공연 관람")
                .build();
        JobDetail PblprfrFcltyDetailInfoJobDetail = JobBuilder.newJob(PblprfrFcltyDetailInfo.class)
                .withIdentity("공연 시설 상세 정보 수집", "공연 관람").build();
        // JobDetail kopisPlaceAllJobDetail = JobBuilder.newJob(KopisPlaceAllJob.class)
        // .withIdentity("공연 시설 통합 추가", "공연관람").build();

        Trigger kopisRegionTrigger = TriggerBuilder.newTrigger().forJob(kopisRegionJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                .build();
        Trigger PblprfrFcltyInfoTrigger = TriggerBuilder.newTrigger().forJob(PblprfrFcltyInfoJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 2 * ?")
                //                 .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        Trigger PblprfrFcltyDetailInfoTrigger = TriggerBuilder.newTrigger().forJob(PblprfrFcltyDetailInfoJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 30 2-7 2 * ?") // 매월 2일 2시부터 7시까지 30분마다 이미 있으면 바로 종료
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();

        // scheduler.scheduleJob(kopisRegionJobDetail, kopisRegionTrigger);
        // scheduler.scheduleJob(PblprfrFcltyInfoJobDetail, PblprfrFcltyInfoTrigger);
        // scheduler.scheduleJob(PblprfrFcltyDetailInfoJobDetail, PblprfrFcltyDetailInfoTrigger);
    }
}
