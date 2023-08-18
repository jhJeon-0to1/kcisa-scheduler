package scheduler.test.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jetbrains.annotations.NotNull;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import scheduler.test.job.db.KopisDBManager;
import scheduler.test.model.SchedulerLog;
import scheduler.test.model.SchedulerStatus;
import scheduler.test.service.SchedulerLogService;

import javax.sql.DataSource;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class KopisJob extends QuartzJobBean {
    private final DataSource dataSource;
    private final SchedulerLogService schedulerLogService;
    private final List<String> sidos = Arrays.asList("^11", "^28", "^41", "^30", "^36", "^44", "^43", "^51|^42", "^27", "^26", "^31", "^48", "^47", "^29", "^46", "^45", "^50");
    private final List<String> ctprvn = Arrays.asList("11", "28", "41", "30", "36", "44", "43", "51", "27", "26", "31", "48", "47", "29", "46", "45", "50");
    private final List<String> sido_names = Arrays.asList("서울시", "인천시", "경기도", "대전시", "세종시", "충청남도", "충청북도", "강원도", "대구시", "부산시", "울산시", "경상남도", "경상북도", "광주시", "전라남도", "전라북도", "제주도");
    String tableName = "pblprfr_viewing_info";
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy.MM.dd");
    WebClient webClient = WebClient.builder().baseUrl("https://www.kopis.or.kr").build();
    ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public KopisJob(DataSource dataSource, SchedulerLogService schedulerLogService) {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;
    }

    @Override
    protected void executeInternal(@NotNull JobExecutionContext context) throws JobExecutionException {
        List<JsonNode> dataList = new ArrayList<>();
        Date yesterDay = new Date(System.currentTimeMillis() - (1000 * 60 * 60 * 24));
        String date = formatter.format(yesterDay);
        String year = date.substring(0, 4);
        String month = date.substring(5, 7);
        String day = date.substring(8, 10);

        String scheduleName = context.getJobDetail().getKey().getName();
        String scheduleGroup = context.getJobDetail().getKey().getGroup();

        schedulerLogService.create(new SchedulerLog(scheduleGroup, scheduleName, tableName, SchedulerStatus.STARTED));
        try {
            for (int i = 0; i < sidos.size(); i++) {
                String sido = sidos.get(i);
                String sido_name = sido_names.get(i);
                String ctprvn_cd = ctprvn.get(i);

                String url = "/por/stats/perfo/perfoStatsTotalList.json";

                Mono<JsonNode> response = webClient.get().uri(uriBuilder -> uriBuilder.path(url).queryParam("startDate", date).queryParam("endDate", date).queryParam("signgu_code", sido).build()).retrieve().bodyToMono(JsonNode.class);

                JsonNode jsonResponse = response.block();

                JsonNode rows = Objects.requireNonNull(jsonResponse).get("result");

                if (rows != null && rows.isArray()) {
                    for (JsonNode row : rows) {
                        ObjectNode newRow = objectMapper.createObjectNode();
                        String nowSido = row.get("signgu_codeNm").asText();
                        String nowGenreCode = row.get("genre_code").asText();

                        if (!Objects.equals(nowSido, "합계") && !Objects.equals(nowGenreCode, "null")) {
                            newRow.put("BASE_DE", date.replace(".", ""));
                            newRow.put("BASE_YEAR", year);
                            newRow.put("BASE_MT", month);
                            newRow.put("BASE_DAY", day);
                            newRow.put("CTPRVN_CD", ctprvn_cd);
                            newRow.put("CTPRVN_NM", sido_name);
                            newRow.put("GENRE_CD", row.get("genre_code").asText());
                            newRow.put("GENRE_NM", row.get("genre_code_nm").asText());
                            newRow.put("PBLPRFR_RASNG_CUTIN_CO", row.get("data1").asText());
//                            newRow.put("PBLPRFR_RASNG_CUTIN_OCCU_RT", row.get("data2").asText());
                            newRow.put("PBLPRFR_CO", row.get("data3").asText());
//                            newRow.put("PBLPRFR_OCCU_RT", row.get("data4").asText());
                            newRow.put("PBLPRFR_STGNG_CO", row.get("data16").asText());
//                            newRow.put("PBLPRFR_STGNG_OCCU_RT", row.get("data17").asText());
                            newRow.put("PBLPRFR_SALES_PRICE", row.get("data5").asText());
//                            newRow.put("PBLPRFR_SALES_PRICE_RT", row.get("data6").asText());
                            newRow.put("PBLPRFR_VIEWNG_NMPR_CO", row.get("data7").asText());
//                            newRow.put("PBLPRFR_VIEWNG_NMPR_RT", row.get("data8").asText());

                            dataList.add(newRow);
                        }

                    }
                } else {
                    throw new Exception("result is null");
                }
            }
            System.out.println(dataList);
            KopisDBManager kopisDbManager = new KopisDBManager(dataSource);
            Map<String, Object> result = kopisDbManager.insertRow(dataList);

            if (result.get("isSuccess").equals(true)) {
//                schdulerLog로 넣기 개수랑 같이
                int count = dataList.size();
                schedulerLogService.create(new SchedulerLog(scheduleGroup, scheduleName, tableName, SchedulerStatus.SUCCESS, count));
                System.out.println("KOPIS 완료");
            } else {
                schedulerLogService.create(new SchedulerLog(scheduleGroup, scheduleName, tableName, SchedulerStatus.FAILED, result.get("message").toString()));
                throw new Exception("DB insert fail");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new JobExecutionException(e);
        }
    }
}


