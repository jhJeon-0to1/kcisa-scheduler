package scheduler.kcisa.configuration.analysis;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.analysis.pet.PetActivateCrstat;
import scheduler.kcisa.job.analysis.pet.PetCtprvnAcctoCrstat;

import javax.annotation.PostConstruct;
import java.util.TimeZone;

@Configuration
public class PetAnalysisConfiguration {
    private final Scheduler scheduler;

    @Autowired
    public PetAnalysisConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void jobRun() throws SchedulerException {
        JobDetail PetCtprvnAcctoCrstatJobDetail = JobBuilder.newJob(PetCtprvnAcctoCrstat.class).
                withIdentity("반려동물 시도별 현황 분석", "반려 동물").
                build();
        Trigger PetCtprvnAcctoCrstatTrigger = TriggerBuilder.newTrigger()
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).withRepeatCount(0))
                // 매월 10일 3시부터 7시까지 0분마다 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 3-7 10 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(PetCtprvnAcctoCrstatJobDetail, PetCtprvnAcctoCrstatTrigger);


        JobDetail PetActivateCrstatJobDetail = JobBuilder.newJob(PetActivateCrstat.class).
                withIdentity("반려동물 활성화 현황 분석", "반려 동물").
                build();
        Trigger PetActivateCrstatTrigger = TriggerBuilder.newTrigger()
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).withRepeatCount(0))
                // 매월 10일 3시부터 7시까지 30분마다 실행 (시도별 현황 분석 이후에 실행)
                .withSchedule(CronScheduleBuilder.cronSchedule("0 30 3-7 10 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(PetActivateCrstatJobDetail, PetActivateCrstatTrigger);
    }

    @PostConstruct
    public void init() throws SchedulerException {
        jobRun();
    }
}
