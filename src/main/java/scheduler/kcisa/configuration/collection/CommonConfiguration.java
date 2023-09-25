package scheduler.kcisa.configuration.collection;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.collection.common.CTPRVNJob;

import javax.annotation.PostConstruct;

@Configuration
public class CommonConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public CommonConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    public void start() throws SchedulerException {
        JobDetail peoplePerCityJobDetail = JobBuilder.newJob(CTPRVNJob.class).withIdentity("시도별 인구 정보 수집", "메타 데이터")
                .build();

        Trigger peoplePerCityTrigger = TriggerBuilder.newTrigger().forJob(peoplePerCityJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).withRepeatCount(0))
                // 매달 1-10일 13시에 (매월 첫 주말, 공휴일 제외한 평일 12시 이후에 업데이트 되므로)
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 13 1-10 *
                // ?").inTimeZone(java.util.TimeZone.getTimeZone("Asia/Seoul")))
                .build();

        // scheduler.scheduleJob(peoplePerCityJobDetail, peoplePerCityTrigger);
    }
}
