package scheduler.kcisa.configuration.collection;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.collection.lsr.LsrExpndtrStdizInfo;
import scheduler.kcisa.job.collection.lsr.LstMvmnQyInfo;

import javax.annotation.PostConstruct;

@Configuration
public class LsrCollectionConfiguration {
    Scheduler scheduler;

    @Autowired
    public LsrCollectionConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    public void start() throws SchedulerException {
        JobDetail lsrExpndtrStdizInfoJobDetail = JobBuilder.newJob(LsrExpndtrStdizInfo.class)
                .withIdentity("문화여가 지출 변동량 정보 수집", "문화여가 지출")
                .build();
        Trigger lsrExpndtrStdizInfoTrigger = TriggerBuilder.newTrigger().forJob(lsrExpndtrStdizInfoJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).withRepeatCount(0))
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 * * ?")) // 매일 새벽 2시에 실행
                .build();
//        scheduler.scheduleJob(lsrExpndtrStdizInfoJobDetail, lsrExpndtrStdizInfoTrigger);

        JobDetail lsrMvmnQyInfoJobDetail = JobBuilder.newJob(LstMvmnQyInfo.class)
                .withIdentity("문화여가 이동양 정보 수집", "문화여가 이동")
                .build();
        Trigger lsrMvmnQyInfoTrigger = TriggerBuilder.newTrigger().forJob(lsrMvmnQyInfoJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).withRepeatCount(0))
//                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 * * ?")) // 매일 새벽 2시에 실행
                .build();
//        scheduler.scheduleJob(lsrMvmnQyInfoJobDetail, lsrMvmnQyInfoTrigger);
    }
}
