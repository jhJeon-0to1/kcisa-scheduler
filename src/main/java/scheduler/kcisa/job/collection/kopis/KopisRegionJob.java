package scheduler.kcisa.job.collection.kopis;

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
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Component
public class KopisRegionJob extends QuartzJobBean {
    private final DataSource dataSource;
    private final SchedulerLogService schedulerLogService;
    private final List<String> sidos = Arrays.asList("^11", "^28", "^41", "^30", "^36", "^44", "^43", "^51|^42", "^27", "^26", "^31", "^48", "^47", "^29", "^46", "^45", "^50");
    private final List<String> ctprvn = Arrays.asList("11", "28", "41", "30", "36", "44", "43", "51", "27", "26", "31", "48", "47", "29", "46", "45", "50");
    private final List<String> sido_names = Arrays.asList("서울시", "인천시", "경기도", "대전시", "세종시", "충청남도", "충청북도", "강원도", "대구시", "부산시", "울산시", "경상남도", "경상북도", "광주시", "전라남도", "전라북도", "제주도");
    String tableName = "pblprfr_viewing_info";
    WebClient webClient = WebClient.builder().baseUrl("https://www.kopis.or.kr").build();
    String url = "/por/stats/perfo/perfoStatsTotalList.json";

    @Autowired
    public KopisRegionJob(DataSource dataSource, SchedulerLogService schedulerLogService) {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        LocalDate yesterDay = LocalDate.now().minusDays(2);
        String date = yesterDay.format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));
        String year = date.substring(0, 4);
        String month = date.substring(5, 7);
        String day = date.substring(8, 10);

        String scheduleName = context.getJobDetail().getKey().getName();
        String scheduleGroup = context.getJobDetail().getKey().getGroup();

        try {
            schedulerLogService.create(new SchedulerLog(scheduleGroup, scheduleName, tableName, SchedulerStatus.STARTED));
            
            int count = 0;
            String insertQuery = "INSERT INTO kcisa.pblprfr_viewing_info (BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD, CTPRVN_NM, GENRE_CD, GENRE_NM, PBLPRFR_RASNG_CUTIN_CO, PBLPRFR_CO, PBLPRFR_STGNG_CO, PBLPRFR_SALES_PRICE, PBLPRFR_VIEWNG_NMPR_CO) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE BASE_YEAR = VALUES(BASE_YEAR), BASE_MT = VALUES(BASE_MT), BASE_DAY = VALUES(BASE_DAY), CTPRVN_NM = VALUES(CTPRVN_NM), GENRE_NM = VALUES(GENRE_NM), PBLPRFR_RASNG_CUTIN_CO = VALUES(PBLPRFR_RASNG_CUTIN_CO), PBLPRFR_CO = VALUES(PBLPRFR_CO), PBLPRFR_STGNG_CO = VALUES(PBLPRFR_STGNG_CO), PBLPRFR_SALES_PRICE = VALUES(PBLPRFR_SALES_PRICE), PBLPRFR_VIEWNG_NMPR_CO = VALUES(PBLPRFR_VIEWNG_NMPR_CO), UPDT_DT = NOW()";

            PreparedStatement pstmt = dataSource.getConnection().prepareStatement(insertQuery);
            for (int i = 0; i < sidos.size(); i++) {
                String sido = sidos.get(i);
                String sido_name = sido_names.get(i);
                String ctprvn_cd = ctprvn.get(i);

                String formData = "startDate=" + date + "&endDate=" + date + "&signgu_code=" + sido;

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
                            pstmt.setString(1, date.replace(".", ""));
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


