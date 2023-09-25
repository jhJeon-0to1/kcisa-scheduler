package scheduler.kcisa.job.collection.kopis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.service.SchedulerLogService;
import scheduler.kcisa.utils.Utils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class KopisPlaceJob extends QuartzJobBean {
    DataSource dataSource;
    SchedulerLogService schedulerLogService;
    @Value("${kopis.api.key}")
    String key;
    WebClient webClient = WebClient.builder().baseUrl("http://kopis.or.kr").build();
    String tableName = "COLCT_PBLPRFR_FCLTY_INFO";
    Connection connection;
    Map<String, String> regionMap = new HashMap<>();

    @Autowired
    public KopisPlaceJob(DataSource dataSource, SchedulerLogService schedulerLogService) {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;

        regionMap.put("해외", "99");
        regionMap.put("서울", "11");
        regionMap.put("인천", "28");
        regionMap.put("경기", "41");
        regionMap.put("전남", "46");
        regionMap.put("대구", "27");
        regionMap.put("경북", "47");
        regionMap.put("대전", "30");
        regionMap.put("광주", "29");
        regionMap.put("울산", "31");
        regionMap.put("부산", "26");
        regionMap.put("충북", "43");
        regionMap.put("충남", "44");
        regionMap.put("경남", "48");
        regionMap.put("제주", "50");
        regionMap.put("강원", "51");
        regionMap.put("세종", "36");
        regionMap.put("전북", "45");
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();
        int count = 0;
        int cPage = 1;

        XmlMapper xmlMapper = new XmlMapper();
        try {
            connection = dataSource.getConnection();
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            String insertQuery = "INSERT INTO COLCT_PBLPRFR_FCLTY_INFO (PBLPRFR_FCLTY_ID,PBLPRFR_FCLTY_NM,PRFPLC_CO,FCLTY_CHARTR,CTPRVN_CD,CTPRVN_NM,SIGNGU_NM,OPNNG_YEAR,COLCT_YM) VALUES (?,?,?,?,?,(SELECT CTPRVN_NM FROM CTPRVN_INFO AS C WHERE C.CTPRVN_CD = ?),?,?,DATE_FORMAT(NOW(), '%Y%m')) ON DUPLICATE KEY UPDATE PBLPRFR_FCLTY_NM=VALUES(PBLPRFR_FCLTY_NM),PRFPLC_CO=VALUES(PRFPLC_CO),FCLTY_CHARTR=VALUES(FCLTY_CHARTR),CTPRVN_NM=VALUES(CTPRVN_NM),SIGNGU_NM=VALUES(SIGNGU_NM),OPNNG_YEAR=VALUES(OPNNG_YEAR),UPDT_YM=DATE_FORMAT(NOW(), '%Y%m')";
            PreparedStatement pstmt = connection.prepareStatement(insertQuery);

            while (true) {
                int finalCPage = cPage;
                String url = "/openApi/restful/prfplc?service=" + key + "&cpage=" + finalCPage + "&rows=100";
                String response = webClient.get().uri(url).retrieve().bodyToMono(String.class).block();

                JsonNode responseJson = xmlMapper.readValue(response, JsonNode.class);

                if (responseJson.has("db")) {
                    for (JsonNode node : responseJson.get("db")) {
                        pstmt.setString(1, node.get("mt10id").asText());
                        pstmt.setString(2, node.get("fcltynm").asText());
                        pstmt.setString(3, node.get("mt13cnt").asText());
                        pstmt.setString(4, node.get("fcltychartr").asText());
                        pstmt.setString(5, regionMap.get(node.get("sidonm").asText()));
                        pstmt.setString(6, regionMap.get(node.get("sidonm").asText()));
                        pstmt.setString(7, node.get("gugunnm").asText());
                        pstmt.setString(8, node.get("opende").asText());

                        pstmt.addBatch();
                        count++;
                    }
                    cPage++;
                } else {
                    break;
                }
            }
            pstmt.executeBatch();

            Optional<Integer> updtCount = Utils.getUpdtCount(tableName);
            if (!updtCount.isPresent()) {
                throw new Exception("getUpdtCount error");
            }

            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count,
                    count - updtCount.get(), updtCount.get()));
        } catch (Exception e) {
            e.printStackTrace();
            schedulerLogService
                    .create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        }
    }
}
