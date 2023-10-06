package scheduler.kcisa.configuration.analysis;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.analysis.pblprfr.*;

import javax.annotation.PostConstruct;

@Configuration
public class PblprfrAnalysisConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public PblprfrAnalysisConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostConstruct
    public void start() throws SchedulerException {
        JobDetail placeMartJobDetail = JobBuilder.newJob(PblprfrFcltyCrstat.class)
                .withIdentity("공연 시설 현황 분석", "공연 관람").build();
        Trigger placeMartTrigger = TriggerBuilder.newTrigger().forJob(placeMartJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 2 * ?") // 매월 2일 새벽 3시부터 7시까지 0분 0초에 실행
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
//                scheduler.scheduleJob(placeMartJobDetail, placeMartTrigger);

        JobDetail PlbprfrActivateCrstatJobDetail = JobBuilder.newJob(PblprfrActivateCrstatJob.class).withIdentity("공연 활성화 현황 분석", "공연 관람").build();
        Trigger PlbprfrActivateCrstatTrigger = TriggerBuilder.newTrigger().forJob(PlbprfrActivateCrstatJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 30 3-7 2 * ?") // 매월 2일 새벽 3시부터 7시까지 30분 0초에 실행 (시설 현황 분석보다 늦게 실행되도록)
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
//        scheduler.scheduleJob(PlbprfrActivateCrstatJobDetail, PlbprfrActivateCrstatTrigger);

        JobDetail PblprfrMtAcctoGenreActtoRasngCutinCrstatJobDetail = JobBuilder.newJob(PblprfrMtAcctoGenreActtoRasngCutinCrstatJob.class).withIdentity("공연 월별 장르별 개막 현황 분석", "공연 관람").build();
        Trigger PblprfrMtAcctoGenreActtoRasngCutinCrstatTrigger = TriggerBuilder.newTrigger().forJob(PblprfrMtAcctoGenreActtoRasngCutinCrstatJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 30 3-7 2 * ?") // 매월 2일 새벽 3시부터 7시까지 30분 0초에 실행 (시설 현황 분석보다 늦게 실행되도록)
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
//        scheduler.scheduleJob(PblprfrMtAcctoGenreActtoRasngCutinCrstatJobDetail, PblprfrMtAcctoGenreActtoRasngCutinCrstatTrigger);

        JobDetail PblPrfrRasnCutinCrstatJobDetail = JobBuilder.newJob(PblprfrRasngCutinCrstat.class).withIdentity("공연 개막 현황 분석", "공연 관람").build();
        Trigger PblPrfrRasnCutinCrstatTrigger = TriggerBuilder.newTrigger().forJob(PblPrfrRasnCutinCrstatJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 30 3-7 2 * ?") // 매월 2일 새벽 3시부터 7시까지 30분 0초에 실행 (시설 현황 분석보다 늦게 실행되도록)
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
//        scheduler.scheduleJob(PblPrfrRasnCutinCrstatJobDetail, PblPrfrRasnCutinCrstatTrigger);

        JobDetail PblprfrViewngCrstatJobDetail = JobBuilder.newJob(PblprfrViewngCrstat.class).withIdentity("공연 개막 현황 분석", "공연 관람").build();
        Trigger PblprfrViewngCrstatTrigger = TriggerBuilder.newTrigger().forJob(PblprfrViewngCrstatJobDetail)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60)
                        .withRepeatCount(0))
                // .withSchedule(CronScheduleBuilder.cronSchedule("0 30 3-7 2 * ?") // 매월 2일 새벽 3시부터 7시까지 30분 0초에 실행 (시설 현황 분석보다 늦게 실행되도록)
                // .inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(PblprfrViewngCrstatJobDetail, PblprfrViewngCrstatTrigger);
    }
}
