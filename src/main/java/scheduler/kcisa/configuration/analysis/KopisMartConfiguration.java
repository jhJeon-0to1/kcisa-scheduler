package scheduler.kcisa.configuration.analysis;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.analysis.kopis.KopisPlaceStateJob;

import javax.annotation.PostConstruct;

@Configuration
public class KopisMartConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public KopisMartConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    public void start() throws SchedulerException {
        JobDetail placeMartJobDetail = JobBuilder.newJob(KopisPlaceStateJob.class)
                .withIdentity("공연 시설 현황 분석", "공연 관람").build();

        Trigger placeMartTrigger = TriggerBuilder.newTrigger().forJob(placeMartJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 2 * ?")
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();

//                scheduler.scheduleJob(placeMartJobDetail, placeMartTrigger);
    }
}
