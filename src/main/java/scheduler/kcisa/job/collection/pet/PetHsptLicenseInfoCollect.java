package scheduler.kcisa.job.collection.pet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.model.flag.collection.MonthlyCollectionFlag;
import scheduler.kcisa.service.flag.collection.MonthlyCollectionFlagService;
import scheduler.kcisa.utils.JobUtils;
import scheduler.kcisa.utils.PetCollection;
import scheduler.kcisa.utils.Utils;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class PetHsptLicenseInfoCollect extends QuartzJobBean {
    MonthlyCollectionFlagService flagService;
    String tableName = "colct_pet_hspt_license_info";
    String url = "https://www.localdata.go.kr/datafile/each/02_03_01_P_CSV.zip";

    public PetHsptLicenseInfoCollect(MonthlyCollectionFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(@NotNull JobExecutionContext jobExecutionContext) throws JobExecutionException {
        LocalDate stdDate = LocalDate.now().minusMonths(1).withDayOfMonth(1);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyyMM"));
        AtomicInteger count = new AtomicInteger();

        JobUtils.executeJob(jobExecutionContext, tableName, jobData -> {
            JsonNode response = PetCollection.getPetData(url, "src/main/resources/data/pet/", stdDateStr + "_PetHsptLicenseInfo.zip", stdDateStr + "_PetHsptLicenseInfo.csv");

            String sql = Utils.getSQLString("src/main/resources/sql/collection/pet/PetHsptLicenseInfo.sql");
            try (PreparedStatement pstmt = jobData.conn.prepareStatement(sql);) {
                ObjectMapper mapper = new ObjectMapper();
                response.fields().forEachRemaining(entry -> {
                    try {
                        String key = entry.getKey();
                        BigDecimal value = mapper.readValue(entry.getValue().toString(), BigDecimal.class);

                        pstmt.setString(1, stdDateStr);
                        pstmt.setString(2, key);
                        pstmt.setString(3, key);
                        pstmt.setBigDecimal(4, value);

                        pstmt.addBatch();
                        count.getAndIncrement();
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });

                pstmt.executeBatch();

                Optional<Integer> updt_cnt = Utils.getUpdtCount(tableName);
                if (!updt_cnt.isPresent()) {
                    throw new Exception("updt_cnt is empty");
                }
                jobData.logService.create(new SchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.SUCCESS, count.get(),
                        count.get() - updt_cnt.get(), updt_cnt.get()));

                System.out.println(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + " 동물병원 수집 완료");

                flagService.create(new MonthlyCollectionFlag(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy")), tableName, true));
            }
        });
    }
}


