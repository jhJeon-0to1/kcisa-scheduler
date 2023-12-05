package scheduler.kcisa.job.collection.pet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.model.flag.collection.MonthlyCollectionFlag;
import scheduler.kcisa.service.flag.collection.MonthlyCollectionFlagService;
import scheduler.kcisa.utils.JobUtils;
import scheduler.kcisa.utils.Utils;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class PetRegistCrstatCollect extends QuartzJobBean {
    MonthlyCollectionFlagService flagService;
    String tableName = "colct_pet_regist_crstat";
    @Value("${pet.api.key}")
    String apiKey;
    String dataCode = "Grid_20210806000000000612_1";
    WebClient webClient = WebClient.builder().baseUrl("http://211.237.50.150:7080/openapi/").build();

    public PetRegistCrstatCollect(MonthlyCollectionFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        String url = apiKey + "/" + "json" + "/" + dataCode;
        AtomicInteger startRow = new AtomicInteger(1);

        LocalDate stdDate = LocalDate.now().minusMonths(1).withDayOfMonth(1);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyyMM"));

        ArrayNode tempData = new ObjectMapper().createArrayNode();

        JobUtils.executeJob(jobExecutionContext, tableName, jobData -> {
            String groupName = jobData.groupName;
            String jobName = jobData.jobName;

            while (true) {
                String fetchUrl = url + "/" + startRow + "/" + (startRow.get() + 999);

                JsonNode response = webClient.get()
                        .uri(fetchUrl)
                        .retrieve()
                        .bodyToMono(JsonNode.class)
                        .block();

                if (response.isEmpty()) {
                    jobData.logService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, "response is empty"));
                }

                JsonNode rows = response.get(dataCode).get("row");
                if (rows.isEmpty()) {
                    break;
                }

                for (JsonNode row : rows) {
                    boolean found = false;

                    for (JsonNode tempRow : tempData) {
                        if (tempRow.get("ctprvn_nm").asText().equals(row.get("CTPV").asText()) && tempRow.get("PET_TY_NM").asText().equals(row.get("LVSTCK_KND").asText())) {
                            found = true;
                            int count = tempRow.get("count").asInt();
                            ((ObjectNode) tempRow).put("count", count + row.get("CNT").asInt());
                            break;
                        }
                    }

                    if (!found) {
                        ObjectNode temp = new ObjectMapper().createObjectNode();
                        temp.put("date", stdDateStr);
                        temp.put("ctprvn_nm", row.get("CTPV").asText());
                        temp.put("PET_TY_NM", row.get("LVSTCK_KND").asText());
                        temp.put("count", row.get("CNT").asText());

                        tempData.add(temp);
                    }
                }
                startRow.addAndGet(1000);
            }
//            tempData에 전국 데이터 넣기
            int dogCount = 0;
            int catCount = 0;
            for (JsonNode tempRow : tempData) {
                if (tempRow.get("PET_TY_NM").asText().equals("개")) {
                    dogCount += tempRow.get("count").asInt();
                } else {
                    catCount += tempRow.get("count").asInt();
                }
            }
            ObjectNode temp = new ObjectMapper().createObjectNode();
            temp.put("date", stdDateStr);
            temp.put("ctprvn_nm", "전국");
            temp.put("PET_TY_NM", "개");
            temp.put("count", dogCount);
            tempData.add(temp);

            temp = new ObjectMapper().createObjectNode();
            temp.put("date", stdDateStr);
            temp.put("ctprvn_nm", "전국");
            temp.put("PET_TY_NM", "고양이");
            temp.put("count", catCount);
            tempData.add(temp);

            String sql = Utils.getSQLString("src/main/resources/sql/collection/pet/PetRegistCrstat.sql");
            try (PreparedStatement pstmt = jobData.conn.prepareStatement(sql)) {
                int insertCount = 0;
                for (JsonNode tempRow : tempData) {
                    String ctprvn_nm = tempRow.get("ctprvn_nm").asText();

                    String PET_TY_NM = tempRow.get("PET_TY_NM").asText();
                    String PET_TY_CD = "";
                    if (PET_TY_NM.equals("개")) {
                        PET_TY_CD = "01";
                    } else {
                        PET_TY_CD = "02";
                    }
                    int count = tempRow.get("count").asInt();

                    pstmt.setString(1, stdDateStr);
                    pstmt.setString(2, ctprvn_nm);
                    pstmt.setString(3, ctprvn_nm);
                    pstmt.setString(4, PET_TY_CD);
                    pstmt.setString(5, PET_TY_NM);
                    pstmt.setBigDecimal(6, new BigDecimal(count));

                    pstmt.addBatch();

                    insertCount++;
                }

                pstmt.executeBatch();

                Optional<Integer> updt_count = Utils.getUpdtCount(tableName);
                if (!updt_count.isPresent()) {
                    throw new Exception("updt_count is null");
                }
                jobData.logService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, insertCount, insertCount - updt_count.get(), updt_count.get()));

                flagService.create(new MonthlyCollectionFlag(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM")), tableName, true));
            }
        });
    }
}
