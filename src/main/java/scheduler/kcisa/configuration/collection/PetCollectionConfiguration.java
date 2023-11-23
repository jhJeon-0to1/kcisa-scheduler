package scheduler.kcisa.configuration.collection;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import scheduler.kcisa.job.collection.pet.PetBtyFcltyLicenseInfoCollect;
import scheduler.kcisa.job.collection.pet.PetConsgnManageFcltyLicenseInfoCollect;
import scheduler.kcisa.job.collection.pet.PetHsptLicenseInfoCollect;
import scheduler.kcisa.job.collection.pet.PetRegistCrstatCollect;

import javax.annotation.PostConstruct;
import java.util.TimeZone;

@Configuration
public class PetCollectionConfiguration {
    Scheduler scheduler;

    @Autowired
    public PetCollectionConfiguration(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void registCollect() throws SchedulerException {
        JobDetail petRegistCrstatCollectJobDetail = JobBuilder.newJob(PetRegistCrstatCollect.class)
                .withIdentity("반려동물 등록 현황 수집", "반려 동물")
                .build();
        Trigger petRegistCrstatCollectTrigger = TriggerBuilder.newTrigger()
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0).withIntervalInSeconds(60))
                // 매월 5일 2시에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 5 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(petRegistCrstatCollectJobDetail, petRegistCrstatCollectTrigger);
    }

    public void licenseCollect() throws SchedulerException {
        JobDetail petHsptLicenseInfoCollectJobDetail = JobBuilder.newJob(PetHsptLicenseInfoCollect.class).withIdentity("반려동물 병원 인허가 정보 수집", "반려 동물").build();
        Trigger petHsptLicenseInfoCollectTrigger = TriggerBuilder.newTrigger()
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0).withIntervalInSeconds(60))
                // 매월 5일 2시에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 5 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(petHsptLicenseInfoCollectJobDetail, petHsptLicenseInfoCollectTrigger);

        JobDetail petBtyFcltyLicenseInfoCollectJobDetail = JobBuilder.newJob(PetBtyFcltyLicenseInfoCollect.class).withIdentity("반려동물 미용 시설 인허가 정보 수집", "반려 동물").build();
        Trigger petBtyFcltyLicenseInfoCollectTrigger = TriggerBuilder.newTrigger()
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0).withIntervalInSeconds(60))
                // 매월 5일 2시에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 5 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(petBtyFcltyLicenseInfoCollectJobDetail, petBtyFcltyLicenseInfoCollectTrigger);

        JobDetail petConsgnManageFcltyLicenseInfoCollectJobDetail = JobBuilder.newJob(PetConsgnManageFcltyLicenseInfoCollect.class).withIdentity("반려동물 위탁 관리 시설 인허가 정보 수집", "반려 동물").build();
        Trigger petConsgnManageFcltyLicenseInfoCollectTrigger = TriggerBuilder.newTrigger()
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0).withIntervalInSeconds(60))
                // 매월 5일 2시에 실행
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 5 * ?").inTimeZone(TimeZone.getTimeZone("Asia/Seoul")))
                .build();
        scheduler.scheduleJob(petConsgnManageFcltyLicenseInfoCollectJobDetail, petConsgnManageFcltyLicenseInfoCollectTrigger);
    }

    @PostConstruct
    public void start() throws SchedulerException {
        registCollect();
        licenseCollect();
    }
}
