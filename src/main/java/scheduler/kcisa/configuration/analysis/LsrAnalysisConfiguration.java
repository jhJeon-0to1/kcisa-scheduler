package scheduler.kcisa.configuration.analysis;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.analysis.lsr.LsrExpndtrStdizInfo;
import scheduler.kcisa.job.analysis.lsr.LsrMvmnQyInfo;

import javax.annotation.PostConstruct;

@Configuration
public class LsrAnalysisConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public LsrAnalysisConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    public void start() throws SchedulerException {
        JobDetail LsrMvmnQyInfoJobDetail = JobBuilder.newJob(LsrMvmnQyInfo.class).withIdentity("문화여가 이동양 정보 분석", "문화여가 이동").build();
        Trigger LsrMvmnQyInfoTrigger = TriggerBuilder.newTrigger().forJob(LsrMvmnQyInfoJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
//                  .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3 * * ?")) // 매일 새벽 3시에 실행 (수집 후 분석)
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
//        scheduler.scheduleJob(LsrMvmnQyInfoJobDetail, LsrMvmnQyInfoTrigger);
        JobDetail LsrExpndtrStdizInfoJobDetail = JobBuilder.newJob(LsrExpndtrStdizInfo.class).withIdentity("문화여가 지출 변동량 분석", "문화여가 지출").build();
        Trigger LsrExpndtrStdizInfoTrigger = TriggerBuilder.newTrigger().forJob(LsrExpndtrStdizInfoJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
//                  .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3 * * ?")) // 매일 새벽 3시에 실행 (수집 후 분석)
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
//        scheduler.scheduleJob(LsrExpndtrStdizInfoJobDetail, LsrExpndtrStdizInfoTrigger);
    }
}
