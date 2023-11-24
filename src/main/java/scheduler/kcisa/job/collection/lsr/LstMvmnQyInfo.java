package scheduler.kcisa.job.collection.lsr;

import com.fasterxml.jackson.databind.JsonNode;
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
public class LstMvmnQyInfo extends QuartzJobBean {
    DailyCollectionFlagService flagService;
    String tableName = "colct_lsr_mvmn_qy_info";
    List<Dstrct> dstrctList;
    WebClient webClient = WebClient.builder().baseUrl("https://data.kostat.go.kr/nowcast").build();

    public LstMvmnQyInfo(DailyCollectionFlagService flagService) {
        this.flagService = flagService;

        this.dstrctList = Arrays.asList(
                new Dstrct("", "전체 지역", "00"),
                new Dstrct("01", "상업지역", "01"),
                new Dstrct("02", "관광지", "02"),
                new Dstrct("03", "대형 아울렛", "03"),
                new Dstrct("04", "사무지역", "04"),
                new Dstrct("05", "레저스포츠시설", "05"),
                new Dstrct("06", "주거지역 등", "06")
        );
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String url = "/listIndcrDataAjax.do";
        List<LsrCtprvn.Ctprvn> ctprvnList = new LsrCtprvn().getLsrCtprvnList();

        JobUtils.executeJob(context, tableName, jobData -> {
            String groupName = jobData.groupName;
            String jobName = jobData.jobName;
            Connection conn = jobData.conn;
            SchedulerLogService schedulerLogService = (SchedulerLogService) jobData.logService;

            int count = 0;

            String sql = Utils.getSQLString("src/main/resources/sql/collection/lsr/LsrMvmnQyInfo.sql");
            try (PreparedStatement pstmt = conn.prepareStatement(sql);) {
                for (LsrCtprvn.Ctprvn city : ctprvnList) {
                    for (Dstrct district : dstrctList) {
                        String formData = "indcr_id=19&mode=&initId=&val1=" + city.getCode() + (district.getCode().equals("") ? "" : "&cd2=A00011&val2=" + district.getCode());

                        JsonNode response = webClient.post()
                                .uri(url)
                                .header("Content-Type", "application/x-www-form-urlencoded")
                                .bodyValue(formData)
                                .retrieve()
                                .bodyToMono(JsonNode.class)
                                .block();
                        JsonNode rows = response.get("data");

                        if (rows == null) {
                            throw new Exception("응답이 없습니다.");
                        }

                        for (JsonNode row : rows) {
                            long timeStamp = (row.get("BASE_DT").asLong() / 1000);
                            String value = row.get("INDCR_VL").asText();
                            Instant instant = Instant.ofEpochSecond(timeStamp);
                            String date = instant.atZone(ZoneId.of("Asia/Seoul")).toLocalDate().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                            String year = date.substring(0, 4);
                            String month = date.substring(4, 6);
                            String day = date.substring(6, 8);

                            pstmt.setString(1, date);
                            pstmt.setString(2, year);
                            pstmt.setString(3, month);
                            pstmt.setString(4, day);
                            pstmt.setString(5, city.getDbCode());
                            pstmt.setString(6, city.getDbCode());
                            pstmt.setString(7, district.getDbCode());
                            pstmt.setString(8, district.getName());
                            pstmt.setBigDecimal(9, new BigDecimal(value));

                            pstmt.addBatch();
                            count++;
                        }
                    }
                }
                pstmt.executeBatch();

                Optional<Integer> updt_count = Utils.getUpdtCount(tableName);
                if (!updt_count.isPresent()) {
                    throw new Exception("updt_count is null");
                }
                schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count, count - updt_count.get(), updt_count.get()));

                flagService.create(new DailyCollectionFlag(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")), tableName, true));
            }
        });
    }

    protected class Dstrct {
        private String code;
        private String name;
        private String dbCode;

        public Dstrct(String code, String name, String dbCode) {
            this.code = code;
            this.name = name;
            this.dbCode = dbCode;
        }

        public String getCode() {
            return code;
        }

        public String getName() {
            return name;
        }

        public String getDbCode() {
            return dbCode;
        }
    }
}
