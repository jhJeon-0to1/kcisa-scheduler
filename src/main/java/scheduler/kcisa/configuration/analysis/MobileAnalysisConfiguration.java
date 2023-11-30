package scheduler.kcisa.configuration.analysis;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.analysis.mobile.MobileAplctnMtAcctoUseTimeCrstat;
import scheduler.kcisa.job.analysis.mobile.MobileAplctnUseTimeCrstat;
import scheduler.kcisa.job.analysis.mobile.MobileEntmntAplctnMtAcctoUseTimeCrstat;
import scheduler.kcisa.job.analysis.mobile.MobileEntmntAplctnUseTimeCrstat;

import javax.annotation.PostConstruct;
import java.util.TimeZone;

@Configuration
public class MobileAnalysisConfiguration {
    Scheduler scheduler;

    @Autowired
    public MobileAnalysisConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }


    public void jobStart() throws SchedulerException {
        JobDetail MobileAplctnUseTimeCrstatJobDetail = JobBuilder.newJob(MobileAplctnUseTimeCrstat.class)
                .withIdentity("모바일 애플리케이션 이용 시간 현황 분석", "모바일 이용량")
                .build();
        Trigger MobileAplctnUseTimeCrstatTrigger = TriggerBuilder.newTrigger().forJob(MobileAplctnUseTimeCrstatJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).withRepeatCount(0))
                // 3시부터 7시까지 매일 0분에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 30 3-7 * * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(MobileAplctnUseTimeCrstatJobDetail, MobileAplctnUseTimeCrstatTrigger);


        JobDetail MobileEntmntAplctnUseTimeCrstatJobDetail = JobBuilder.newJob(MobileEntmntAplctnUseTimeCrstat.class).withIdentity("모바일 엔터테인먼트 애플리케이션 이용 시간 현황 분석", "모바일 이용량").build();
        Trigger MobileEntmntAplctnUseTimeCrstatTrigger = TriggerBuilder.newTrigger().forJob(MobileEntmntAplctnUseTimeCrstatJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).withRepeatCount(0))
                // 3시부터 7시까지 매일 0분에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 * * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(MobileEntmntAplctnUseTimeCrstatJobDetail, MobileEntmntAplctnUseTimeCrstatTrigger);

        JobDetail MobileAplctnMtAcctoUseTimeCrstatJobDetail = JobBuilder.newJob(MobileAplctnMtAcctoUseTimeCrstat.class).withIdentity("모바일 애플리케이션 월별 이용 시간 현황 분석", "모바일 이용량").build();
        Trigger MobileAplctnMtAcctoUseTimeCrstatTrigger = TriggerBuilder.newTrigger().forJob(MobileAplctnMtAcctoUseTimeCrstatJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).withRepeatCount(0))
                // 매월 10일 3시부터 7시까지 0분에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 30 3-7 10 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(MobileAplctnMtAcctoUseTimeCrstatJobDetail, MobileAplctnMtAcctoUseTimeCrstatTrigger);

        JobDetail MobileEntmntAplctnMtAcctoUseTimeCrstatJobDetail = JobBuilder.newJob(MobileEntmntAplctnMtAcctoUseTimeCrstat.class).withIdentity("모바일 엔터테인먼트 애플리케이션 월별 이용 시간 현황 분석", "모바일 이용량").build();
        Trigger MobileEntmntAplctnMtAcctoUseTimeCrstatTrigger = TriggerBuilder.newTrigger().forJob(MobileEntmntAplctnMtAcctoUseTimeCrstatJobDetail)
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).withRepeatCount(0))
                // 매월 10일 3시부터 7시까지 0분에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 10 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(MobileEntmntAplctnMtAcctoUseTimeCrstatJobDetail, MobileEntmntAplctnMtAcctoUseTimeCrstatTrigger);
    }

    @PostConstruct
    public void start() throws SchedulerException {
        jobStart();
    }
}
