package scheduler.kcisa.job.collection.pblprfr;

import com.fasterxml.jackson.databind.JsonNode;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.service.SchedulerLogService;
import scheduler.kcisa.utils.CustomException;
import scheduler.kcisa.utils.Utils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Component
public class PlbprfrViewngCtprvnAcctoStat extends QuartzJobBean {
    private final List<String> sidos = Arrays.asList("^11", "^28", "^41", "^30", "^36", "^44", "^43", "^51|^42", "^27", "^26", "^31", "^48", "^47", "^29", "^46", "^45", "^50");
    private final List<String> ctprvn = Arrays.asList("11", "28", "41", "30", "36", "44", "43", "51", "27", "26", "31", "48", "47", "29", "46", "45", "50");
    private final List<String> sido_names = Arrays.asList("서울시", "인천시", "경기도", "대전시", "세종시", "충청남도", "충청북도", "강원도", "대구시", "부산시", "울산시", "경상남도", "경상북도", "광주시", "전라남도", "전라북도", "제주도");
    DataSource dataSource;
    SchedulerLogService schedulerLogService;
    Connection conn = null;
    String tableName = "PBLPRFR_VIEWNG_CTPRVN_ACCTO_STAT";
    WebClient webClient = WebClient.builder().baseUrl("https://www.kopis.or.kr").build();
    String url = "/por/stats/perfo/perfoStatsTotalList.json";

    @Autowired
    public PlbprfrViewngCtprvnAcctoStat(DataSource dataSource, SchedulerLogService schedulerLogService) {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        LocalDate stdDate = LocalDate.now().minusDays(2);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));
        String year = stdDateStr.substring(0, 4);
        String month = stdDateStr.substring(5, 7);
        String day = stdDateStr.substring(8, 10);

        String scheduleName = context.getJobDetail().getKey().getName();
        String scheduleGroup = context.getJobDetail().getKey().getGroup();

        try {
            conn = dataSource.getConnection();
            schedulerLogService.create(new SchedulerLog(scheduleGroup, scheduleName, tableName, SchedulerStatus.STARTED));

            int count = 0;
            String query = Utils.getSQLString("/sql/collection/pblprfr/PlbprfrViewngCtprvnAcctoStat.sql");

            PreparedStatement pstmt = conn.prepareStatement(query);
            for (int i = 0; i < sidos.size(); i++) {
                String sido = sidos.get(i);
                String sido_name = sido_names.get(i);
                String ctprvn_cd = ctprvn.get(i);

                String formData = "startDate=" + stdDateStr + "&endDate=" + stdDateStr + "&signgu_code=" + sido;

                JsonNode rows;
                try {
                    JsonNode response = webClient.post().uri(url).contentType(MediaType.APPLICATION_FORM_URLENCODED).bodyValue(formData).retrieve().bodyToMono(JsonNode.class).block();

                    rows = Objects.requireNonNull(response).get("result");
                } catch (Exception e) {
                    throw new CustomException("001", e.getMessage().substring(0, 300));
                }

                if (rows != null && rows.isArray()) {
                    for (JsonNode row : rows) {
                        String nowSido = row.get("signgu_codeNm").asText();
                        String nowGenreCode = row.get("genre_code").asText();

                        if (!Objects.equals(nowSido, "합계") && !Objects.equals(nowGenreCode, "null")) {
                            pstmt.setString(1, stdDateStr.replace(".", ""));
                            pstmt.setString(2, year);
                            pstmt.setString(3, month);
                            pstmt.setString(4, day);
                            pstmt.setString(5, ctprvn_cd);
                            pstmt.setString(6, sido_name);
                            pstmt.setString(7, row.get("genre_code").asText());
                            pstmt.setString(8, row.get("genre_code_nm").asText());
                            pstmt.setBigDecimal(9, new BigDecimal(row.get("data1").asText()));
//                            pstmt.setBigDecimal(10, new BigDecimal(row.get("data2").asText()));
                            pstmt.setBigDecimal(10, new BigDecimal(row.get("data3").asText()));
//                            pstmt.setBigDecimal(12, new BigDecimal(row.get("data4").asText()));
                            pstmt.setBigDecimal(11, new BigDecimal(row.get("data16").asText()));
//                            pstmt.setBigDecimal(14, new BigDecimal(row.get("data17").asText()));
                            pstmt.setBigDecimal(12, new BigDecimal(row.get("data5").asText()));
//                            pstmt.setBigDecimal(16, new BigDecimal(row.get("data6").asText()));
                            pstmt.setBigDecimal(13, new BigDecimal(row.get("data7").asText()));
//                            pstmt.setBigDecimal(18, new BigDecimal(row.get("data8").asText()));

                            pstmt.addBatch();
                            count++;
                        }
                    }
                } else {
                    throw new Exception("result is null");
                }
            }
            pstmt.executeBatch();

            Optional<Integer> updt_count = Utils.getUpdtCount(tableName);
            if (!updt_count.isPresent()) {
                throw new Exception("updt_count is null");
            }
            schedulerLogService.create(new SchedulerLog(scheduleGroup, scheduleName, tableName, SchedulerStatus.SUCCESS, count, count - updt_count.get(), updt_count.get()));
            System.out.println("KOPIS 완료");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("KOPIS 에러");
            schedulerLogService.create(new SchedulerLog(scheduleGroup, scheduleName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        }
    }
}


