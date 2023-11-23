package scheduler.kcisa.configuration.analysis;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.analysis.lsr.LsrEventInfo;
import scheduler.kcisa.job.analysis.lsr.LsrExpndtrStdizInfo;
import scheduler.kcisa.job.analysis.lsr.LsrMvmnQyInfo;

import javax.annotation.PostConstruct;
import java.util.TimeZone;

@Configuration
public class LsrAnalysisConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public LsrAnalysisConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void jobStart() throws SchedulerException {
        JobDetail LsrMvmnQyInfoJobDetail = JobBuilder.newJob(LsrMvmnQyInfo.class).withIdentity("문화여가 이동양 정보 분석", "문화여가 이동").build();
        Trigger LsrMvmnQyInfoTrigger = TriggerBuilder.newTrigger().forJob(LsrMvmnQyInfoJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매일 3-7시 사이에 1시간마다 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 * * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(LsrMvmnQyInfoJobDetail, LsrMvmnQyInfoTrigger);

        JobDetail LsrExpndtrStdizInfoJobDetail = JobBuilder.newJob(LsrExpndtrStdizInfo.class).withIdentity("문화여가 지출 변동량 분석", "문화여가 지출").build();
        Trigger LsrExpndtrStdizInfoTrigger = TriggerBuilder.newTrigger().forJob(LsrExpndtrStdizInfoJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매일 3-7시 사이에 1시간마다 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 * * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(LsrExpndtrStdizInfoJobDetail, LsrExpndtrStdizInfoTrigger);

        JobDetail LsrEventInfoJobDetail = JobBuilder.newJob(LsrEventInfo.class).withIdentity("문화여가 이벤트 정보 기준 일자 업데이트", "문화여가 이벤트").build();
        Trigger LsrEventInfoTrigger = TriggerBuilder.newTrigger().forJob(LsrEventInfoJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).withRepeatCount(0))
                // 매일 3-7시 사이에 매 30분마다 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 30 3-7 * * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(LsrEventInfoJobDetail, LsrEventInfoTrigger);
    }

    @PostConstruct
    public void start() throws SchedulerException {
        jobStart();
    }
}
