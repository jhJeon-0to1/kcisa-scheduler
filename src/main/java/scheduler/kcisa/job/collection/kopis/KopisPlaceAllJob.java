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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class KopisPlaceAllJob extends QuartzJobBean {
    DataSource dataSource;
    SchedulerLogService schedulerLogService;
    @Value("${kopis.api.key}")
    String key;
    WebClient webClient = WebClient.builder().baseUrl("http://kopis.or.kr").build();
    String tableName = "kopis_공연시설통합";
    Connection connection;
    Map<String, String> regionMap = new HashMap<>();

    @Autowired
    public KopisPlaceAllJob(DataSource dataSource, SchedulerLogService schedulerLogService) {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;

        regionMap.put("해외", "00");
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

            String insertQuery = "INSERT INTO kcisa.kopis_공연시설통합 (fclty_id, fclty_nm, fclty_co, fclty_chartrt, ctprvn_cd, ctprvn_nm, sigungu_nm, open_year, seat_co, tel_no, hmpg_url, fclty_addr, fclty_la, fclty_lo) VALUE (?, ?, ?, ?, ?, (SELECT CTPRVN_INFO.CTPRVN_NM FROM kcisa.CTPRVN_INFO WHERE CTPRVN_INFO.CTPRVN_CD = ?), ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE fclty_nm = VALUES(fclty_nm), fclty_co = VALUES(fclty_co), fclty_chartrt = VALUES(fclty_chartrt), ctprvn_nm = VALUES(ctprvn_nm), sigungu_nm = VALUES(sigungu_nm), open_year = VALUES(open_year), seat_co = VALUES(seat_co), tel_no = VALUES(tel_no), hmpg_url = VALUES(hmpg_url), fclty_addr = VALUES(fclty_addr), fclty_la = VALUES(fclty_la), fclty_lo = VALUES(fclty_lo), updt_dt = NOW()";
            PreparedStatement pstmt = connection.prepareStatement(insertQuery);

            while (true) {
                int finalCPage = cPage;
//                UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl("/openApi/restful/prfplc").queryParam("cpage", finalCPage).queryParam("rows", 100).queryParam("service", key);
                String url = "/openApi/restful/prfplc?cpage=" + finalCPage + "&rows=100&service=" + key;
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

                        String detailUrl = "/openApi/restful/prfplc/" + node.get("mt10id").asText() + "?service=" + key;
                        String detailResponse = webClient.get().uri(detailUrl).retrieve().bodyToMono(String.class).block();
                        JsonNode detailResponseJson = xmlMapper.readValue(detailResponse, JsonNode.class);
                        if (detailResponseJson.has("db")) {
                            JsonNode detailNode = detailResponseJson.get("db");
                            pstmt.setBigDecimal(9, detailNode.get("seatscale").decimalValue());
                            pstmt.setString(10, detailNode.get("telno").asText());
                            pstmt.setString(11, detailNode.get("relateurl").asText());
                            pstmt.setString(12, detailNode.get("adres").asText());
                            pstmt.setBigDecimal(13, detailNode.get("la").decimalValue());
                            pstmt.setBigDecimal(14, detailNode.get("lo").decimalValue());
                        } else {
                            throw new Exception(node.get("mt10id") + " detail job error");
                        }
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

            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count, count - updtCount.get(), updtCount.get()));
        } catch (Exception e) {
            e.printStackTrace();
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        }
    }
}
