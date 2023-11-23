package scheduler.kcisa.job.collection.lsr;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.model.flag.collection.DailyCollectionFlag;
import scheduler.kcisa.service.SchedulerLogService;
import scheduler.kcisa.service.flag.collection.DailyCollectionFlagService;
import scheduler.kcisa.utils.JobUtils;
import scheduler.kcisa.utils.Utils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Component
public class LsrExpndtrStdizInfo extends QuartzJobBean {
    DailyCollectionFlagService flagService;
    String tableName = "colct_lsr_expndtr_stdiz_info";
    List<LsrInduty> indutyList;
    WebClient webClient = WebClient.builder().baseUrl("https://data.kostat.go.kr/nowcast").build();

    public LsrExpndtrStdizInfo(DailyCollectionFlagService flagService) {
        this.flagService = flagService;

        this.indutyList = Arrays.asList(
                new LsrInduty("전체 업종", "", "00"),
                new LsrInduty("식료품", "01", "01"),
                new LsrInduty("의류", "03", "02"),
                new LsrInduty("보건", "06", "03"),
                new LsrInduty("오락 스포츠", "09", "04"),
                new LsrInduty("교육", "10", "05"),
                new LsrInduty("음식 및 음료", "111", "06"),
                new LsrInduty("숙박", "112", "07")
        );
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        List<LsrCtprvn.Ctprvn> ctprvnList = new LsrCtprvn().getLsrCtprvnList();
        String url = "/listIndcrDataAjax.do";

        JobUtils.executeJob(context, tableName, jobData -> {
            String groupName = jobData.groupName;
            String jobName = jobData.jobName;
            Connection conn = jobData.conn;
            SchedulerLogService schedulerLogService = (SchedulerLogService) jobData.logService;

            int count = 0;

            String query = Utils.getSQLString("src/main/resources/sql/collection/lsr/LsrExpndtrStdizInfo.sql");
            try (PreparedStatement pstmt = conn.prepareStatement(query);) {
                for (LsrCtprvn.Ctprvn city : ctprvnList) {
                    for (LsrInduty induty : indutyList) {
                        String formData = "indcr_id=1&mode=&initId=&val1=" + city.getCode() + ((induty.getCode().equals("")) ? "" : "&cd2=A00029&val2=" + induty.getCode());

                        JsonNode response = webClient.post()
                                .uri(url)
                                .header("Content-Type", "application/x-www-form-urlencoded")
                                .bodyValue(formData)
                                .retrieve()
                                .bodyToMono(JsonNode.class)
                                .block();

                        if (response != null) {
                            JsonNode rows = response.get("data");
                            System.out.println(city.getCode() + " " + induty.getCode() + " " + rows.size() + " 수집완료");

                            for (JsonNode row : rows) {
                                long timeStamp = (row.get("BASE_DT").asLong() / 1000);
                                String value = row.get("INDCR_VL").asText();
                                Instant instant = Instant.ofEpochSecond(timeStamp);
                                String date = instant.atZone(ZoneId.of("Asia/Seoul")).toLocalDate().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                                String year = date.substring(0, 4);
                                String month = date.substring(4, 6);
                                String day = date.substring(6, 8);
                                String indutyName = induty.getName();

                                pstmt.setString(1, date);
                                pstmt.setString(2, year);
                                pstmt.setString(3, month);
                                pstmt.setString(4, day);
                                pstmt.setString(5, (city.getDbCode()));
                                pstmt.setString(6, (city.getDbCode()));
                                pstmt.setString(7, induty.getDbCode());
                                pstmt.setString(8, indutyName);
                                pstmt.setBigDecimal(9, new BigDecimal(value));

                                pstmt.addBatch();
                                count++;
                            }
                        } else {
                            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, "응답이 없습니다."));
                        }
                    }
                }
                pstmt.executeBatch();

                Optional<Integer> updt_count = Utils.getUpdtCount(tableName);
                if (!updt_count.isPresent()) {
                    throw new Exception("updt_count is null");
                }
                System.out.println(tableName + " : " + count + "건 수집 완료");
                schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count, count - updt_count.get(), updt_count.get()));

                flagService.create(new DailyCollectionFlag(LocalDate.now(), tableName, true));
            }
        });
    }

    private class LsrInduty {
        @Getter
        private final String code;
        @Getter
        private final String name;
        @Getter
        private final String dbCode;

        public LsrInduty(String name, String code, String dbCode) {
            this.code = code;
            this.name = name;
            this.dbCode = dbCode;
        }
    }
}
