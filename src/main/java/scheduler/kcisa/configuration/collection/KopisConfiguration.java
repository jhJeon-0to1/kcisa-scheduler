package scheduler.kcisa.configuration.collection;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import scheduler.kcisa.job.collection.kopis.KopisPlaceDetailJob;
import scheduler.kcisa.job.collection.kopis.KopisPlaceJob;
import scheduler.kcisa.job.collection.kopis.KopisRegionJob;

import java.util.TimeZone;

import javax.annotation.PostConstruct;

@Configuration
public class KopisConfiguration {
        private final Scheduler scheduler;

        @Autowired
        public KopisConfiguration(Scheduler scheduler) {
                this.scheduler = scheduler;
        }

        @PostConstruct
        public void start() throws SchedulerException {
                JobDetail kopisRegionJobDetail = JobBuilder.newJob(KopisRegionJob.class)
                                .withIdentity("공연 지역별 관중 추가", "TEST").build();
                JobDetail kopisPlaceJobDetail = JobBuilder.newJob(KopisPlaceJob.class)
                                .withIdentity("공연 시설 정보 수집", "공연 관람")
                                .build();
                JobDetail kopisPlaceDetailJobDetail = JobBuilder.newJob(KopisPlaceDetailJob.class)
                                .withIdentity("공연 시설 상세 정보 수집", "공연 관람").build();
                // JobDetail kopisPlaceAllJobDetail = JobBuilder.newJob(KopisPlaceAllJob.class)
                // .withIdentity("공연 시설 통합 추가", "공연관람").build();

                Trigger kopisRegionTrigger = TriggerBuilder.newTrigger().forJob(kopisRegionJobDetail)
                                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                                                .withRepeatCount(0))
                                .build();
                Trigger kopisPlaceTrigger = TriggerBuilder.newTrigger().forJob(kopisPlaceJobDetail)
                                // .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                                // .withRepeatCount(0))
                                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 2 * ?")
                                                .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                                .build();
                Trigger kopisPlaceDescTrigger = TriggerBuilder.newTrigger().forJob(kopisPlaceDetailJobDetail)
                                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                                                .withRepeatCount(0))
                                // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 2 * ?")
                                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                                .build();

                // scheduler.scheduleJob(kopisRegionJobDetail, kopisRegionTrigger);
                // scheduler.scheduleJob(kopisPlaceJobDetail, kopisPlaceTrigger);
                // scheduler.scheduleJob(kopisPlaceDetailJobDetail, kopisPlaceDescTrigger);
        }
}
