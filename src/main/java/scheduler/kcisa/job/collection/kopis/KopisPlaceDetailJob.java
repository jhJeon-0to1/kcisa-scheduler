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
import scheduler.kcisa.utils.CustomException;
import scheduler.kcisa.utils.Utils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Optional;

@Component
public class KopisPlaceDetailJob extends QuartzJobBean {
    DataSource dataSource;
    SchedulerLogService schedulerLogService;
    @Value("${kopis.api.key}")
    String key;
    Connection connection;
    WebClient webClient = WebClient.builder().baseUrl("http://kopis.or.kr/openApi/restful/prfplc").build();

    @Autowired
    public KopisPlaceDetailJob(DataSource dataSource, SchedulerLogService schedulerLogService) {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();
        String tableName = "kopis_공연시설상세";
        XmlMapper xmlMapper = new XmlMapper();
        String date = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));


        try {
            int count = 0;
            connection = dataSource.getConnection();

            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            String countQuery = "SELECT COUNT(*) AS count FROM kcisa.kopis_공연시설 WHERE REGIST_DE = ?";
            PreparedStatement countPstmt = connection.prepareStatement(countQuery);
            countPstmt.setString(1, date);
            ResultSet countRs = countPstmt.executeQuery();
            int nowCount = 0;
            if (countRs.next()) {
                nowCount = countRs.getInt("count");
            }
            countPstmt.close();

            if (nowCount == 0) {
                throw new CustomException("001", "시설정보가 업데이트되지 않았습니다.");
            }


            String selectQuery = "SELECT mt10id FROM kcisa.kopis_공연시설";
            PreparedStatement selectPstmt = connection.prepareStatement(selectQuery);
            ResultSet rs = selectPstmt.executeQuery();
            ArrayList<String> mt10ids = new ArrayList<>();
            while (rs.next()) {
                mt10ids.add(rs.getString("mt10id"));
            }
            selectPstmt.close();

//            String insertQuery = "INSERT INTO kcisa.kopis_공연시설상세 (REGIST_DE, fcltynm, mt10id, mt13cnt, fcltychartr, opende, seatscale, telno, relateurl, adres, la, lo) VALUE  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE fcltynm = VALUES(fcltynm), mt13cnt = VALUES(mt13cnt), fcltychartr = VALUES(fcltychartr), opende = VALUES(opende), seatscale = VALUES(seatscale), telno = VALUES(telno), relateurl = VALUES(relateurl), adres = VALUES(adres), la = VALUES(la), lo = VALUES(lo), updt_dt = NOW()";
            String insertQuery = "INSERT INTO kcisa.kopis_공연시설상세 (REGIST_DE, fcltynm, mt10id, mt13cnt, fcltychartr, opende, seatscale, telno, relateurl, adres, la, lo) VALUE  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement pstmt = connection.prepareStatement(insertQuery);

            for (String id : mt10ids) {
                String response = webClient.get().uri("/" + id + "?service=" + key).retrieve().bodyToMono(String.class).block();

                JsonNode responseJson = xmlMapper.readValue(response, JsonNode.class);
                if (responseJson.has("db")) {
                    JsonNode row = responseJson.get("db");

                    pstmt.setString(1, date);
                    pstmt.setString(2, row.get("fcltynm").asText());
                    pstmt.setString(3, row.get("mt10id").asText());
                    pstmt.setBigDecimal(4, new BigDecimal(row.get("mt13cnt").asText()));
                    pstmt.setString(5, row.get("fcltychartr").asText());
                    pstmt.setString(6, row.get("opende").asText());
                    pstmt.setBigDecimal(7, new BigDecimal(row.get("seatscale").asText()));
                    pstmt.setString(8, row.get("telno").asText());
                    pstmt.setString(9, row.get("relateurl").asText());
                    pstmt.setString(10, row.get("adres").asText());
                    pstmt.setBigDecimal(11, new BigDecimal(row.get("la").asText()));
                    pstmt.setBigDecimal(12, new BigDecimal(row.get("lo").asText()));

                    pstmt.addBatch();
                    count++;
                }
            }
            pstmt.executeBatch();

            Optional<Integer> updtCount = Utils.getUpdtCount(tableName);
            if (!updtCount.isPresent()) {
                throw new Exception("Failed to get updtCount");
            }

            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count, count - updtCount.get(), updtCount.get()));


            // 월별 규모별 극장수 데이터 테이블 생성 쿼리 추가
        } catch (Exception e) {
            e.printStackTrace();
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        }
    }
}
