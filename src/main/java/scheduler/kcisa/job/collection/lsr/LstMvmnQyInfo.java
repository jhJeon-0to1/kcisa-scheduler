package scheduler.kcisa.job.collection.lsr;

import com.fasterxml.jackson.databind.JsonNode;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.service.SchedulerLogService;
import scheduler.kcisa.utils.Utils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Component
public class LstMvmnQyInfo extends QuartzJobBean {
    DataSource dataSource;
    SchedulerLogService schedulerLogService;
    String tableName = "COLCT_LSR_MVMN_QY_INFO";
    List<Dstrct> dstrctList;
    Connection conn;
    WebClient webClient = WebClient.builder().baseUrl("https://data.kostat.go.kr/nowcast").build();

    public LstMvmnQyInfo(DataSource dataSource, SchedulerLogService schedulerLogService) {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;

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
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();
        String url = "/listIndcrDataAjax.do";
        List<LsrCtprvn.Ctprvn> ctprvnList = new LsrCtprvn().getLsrCtprvnList();

        try {
            int count = 0;
            conn = dataSource.getConnection();

            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            String sql = Utils.getSQLString("src/main/resources/sql/collection/lsr/LsrMvmnQyInfo.sql");
            PreparedStatement pstmt = conn.prepareStatement(sql);

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
        } catch (Exception e) {
            e.printStackTrace();
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        } finally {
            try {
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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
