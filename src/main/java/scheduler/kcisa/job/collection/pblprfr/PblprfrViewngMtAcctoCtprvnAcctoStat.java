package scheduler.kcisa.job.collection.pblprfr;

import com.fasterxml.jackson.databind.JsonNode;
import org.quartz.JobExecutionContext;
import org.springframework.http.MediaType;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.model.flag.collection.MonthlyCollectionFlag;
import scheduler.kcisa.service.flag.collection.MonthlyCollectionFlagService;
import scheduler.kcisa.utils.CustomException;
import scheduler.kcisa.utils.JobUtils;
import scheduler.kcisa.utils.Utils;

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
public class PblprfrViewngMtAcctoCtprvnAcctoStat extends QuartzJobBean {
    private final List<String> sidos = Arrays.asList("^11", "^28", "^41", "^30", "^36", "^44", "^43", "^51|^42", "^27", "^26", "^31", "^48", "^47", "^29", "^46", "^45", "^50");
    private final List<String> ctprvn = Arrays.asList("11", "28", "41", "30", "36", "44", "43", "51", "27", "26", "31", "48", "47", "29", "46", "45", "50");

    MonthlyCollectionFlagService flagService;
    String tableName = "colct_pblprfr_viewng_mt_accto_ctprvn_accto_stats";
    WebClient webClient = WebClient.builder().baseUrl("https://www.kopis.or.kr").build();
    String url = "/por/stats/perfo/perfoStatsTotalList.json";

    public PblprfrViewngMtAcctoCtprvnAcctoStat(MonthlyCollectionFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        LocalDate stdDate = LocalDate.now().minusMonths(1).withDayOfMonth(1);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));
        String year = stdDateStr.substring(0, 4);
        String month = stdDateStr.substring(5, 7);

        LocalDate endDate = stdDate.plusMonths(1).minusDays(1);
        String endDateStr = endDate.format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));

        JobUtils.executeJob(context, tableName, jobData -> {
            Connection conn = jobData.conn;

            int count = 0;
            String query = Utils.getSQLString("src/main/resources/sql/collection/pblprfr/PblprfrViewngMtAcctoCtprvnAcctoStat.sql");

            try (PreparedStatement pstmt = conn.prepareStatement(query);) {
                for (int i = 0; i < sidos.size(); i++) {
                    String sido = sidos.get(i);
                    String ctprvn_cd = ctprvn.get(i);

                    String formData = "startDate=" + stdDateStr + "&endDate=" + endDateStr + "&signgu_code=" + sido;

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
                                pstmt.setString(1, year + month);
                                pstmt.setString(2, year);
                                pstmt.setString(3, month);
                                pstmt.setString(4, ctprvn_cd);
                                pstmt.setString(5, ctprvn_cd);
                                pstmt.setString(6, row.get("genre_code").asText());
                                pstmt.setString(7, row.get("genre_code_nm").asText());
                                pstmt.setBigDecimal(8, new BigDecimal(row.get("data1").asText()));  // 개막
                                pstmt.setBigDecimal(9, new BigDecimal(row.get("data3").asText())); // 상영
                                pstmt.setBigDecimal(10, new BigDecimal(row.get("data5").asText())); //매출액
                                pstmt.setBigDecimal(11, new BigDecimal(row.get("data7").asText())); //관객수
                                pstmt.setBigDecimal(12, new BigDecimal(row.get("data16").asText())); // 공연수

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
                jobData.logService.create(new SchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.SUCCESS, count, count - updt_count.get(), updt_count.get()));

                flagService.create(new MonthlyCollectionFlag(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM")), tableName, true));
            }
        });
    }
}


