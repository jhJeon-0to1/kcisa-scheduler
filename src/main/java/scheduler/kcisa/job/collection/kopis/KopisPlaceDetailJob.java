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

    protected void checkFcltyCollect(String date) throws Exception {
        String countQuery = "SELECT COUNT(*) AS count FROM COLCT_PBLPRFR_FCLTY_INFO WHERE COLCT_DE = ?";

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
    }

    protected Boolean checkAlreadyCollect(String date) throws Exception {
        String countQuery = "SELECT COUNT(*) AS count FROM COLCT_PBLPRFR_FCLTY_DETAIL_INFO WHERE COLCT_DE = ?";

        PreparedStatement countPstmt = connection.prepareStatement(countQuery);
        countPstmt.setString(1, date);

        ResultSet countRs = countPstmt.executeQuery();
        int nowCount = 0;
        if (countRs.next()) {
            nowCount = countRs.getInt("count");
        }
        countPstmt.close();

        if (nowCount > 0) {
            return true;
        } else {
            return false;
        }
    }

    protected ArrayList<String> getFcltyId(String date) throws Exception {
        String selectIDQuery = "SELECT PBLPRFR_FCLTY_ID FROM COLCT_PBLPRFR_FCLTY_INFO WHERE COLCT_DE = ?";

        PreparedStatement selectIDPstmt = connection.prepareStatement(selectIDQuery);
        selectIDPstmt.setString(1, date);

        ResultSet rs = selectIDPstmt.executeQuery();
        ArrayList<String> idList = new ArrayList<>();
        while (rs.next()) {
            idList.add(rs.getString("PBLPRFR_FCLTY_ID"));
        }
        selectIDPstmt.close();

        return idList;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();
        String tableName = "COLCT_PBLPRFR_FCLTY_DETAIL_INFO";
        XmlMapper xmlMapper = new XmlMapper();
        String date = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        try {
            int count = 0;
            connection = dataSource.getConnection();

            checkFcltyCollect(date);

            Boolean isAlreadyCollect = checkAlreadyCollect(date);

            if (isAlreadyCollect) {
                System.out.println("이미 수집된 데이터입니다.");

                return;
            }

            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            ArrayList<String> idList = getFcltyId(date);

            String insertQuery = "INSERT INTO COLCT_PBLPRFR_FCLTY_DETAIL_INFO (PBLPRFR_FCLTY_ID,PBLPRFR_FCLTY_NM,PRFPLC_CO,FCLTY_CHARTR,OPNNG_YEAR,FCLTY_SEAT_CO,FCLTY_TEL_NO,FCLTY_URL,FCLTY_ADDR,FCLTY_LA,FLCTY_LO,COLCT_YM) VALUES (?,?,?,?,?,?,?,?,?,?,?,DATE_FORMAT(NOW(), '%Y%m')) ON DUPLICATE KEY UPDATE PBLPRFR_FCLTY_NM=VALUES(PBLPRFR_FCLTY_NM),PRFPLC_CO=VALUES(PRFPLC_CO),FCLTY_CHARTR=VALUES(FCLTY_CHARTR),OPNNG_YEAR=VALUES(OPNNG_YEAR),FCLTY_SEAT_CO=VALUES(FCLTY_SEAT_CO),FCLTY_TEL_NO=VALUES(FCLTY_TEL_NO),FCLTY_URL=VALUES(FCLTY_URL),FCLTY_ADDR=VALUES(FCLTY_ADDR),FCLTY_LA=VALUES(FCLTY_LA),FLCTY_LO=VALUES(FLCTY_LO),UPDT_YM=DATE_FORMAT(NOW(), '%Y%m')";

            PreparedStatement pstmt = connection.prepareStatement(insertQuery);

            for (String id : idList) {
                String response = webClient.get().uri("/" + id + "?service=" + key).retrieve().bodyToMono(String.class)
                        .block();

                JsonNode responseJson = xmlMapper.readValue(response, JsonNode.class);

                if (responseJson.has("db")) {
                    JsonNode row = responseJson.get("db");

                    pstmt.setString(1, row.get("mt10id").asText());
                    pstmt.setString(2, row.get("fcltynm").asText());
                    pstmt.setBigDecimal(3, new BigDecimal(row.get("mt13cnt").asText()));
                    pstmt.setString(4, row.get("fcltychartr").asText());
                    pstmt.setString(5, row.get("opende").asText());
                    pstmt.setBigDecimal(6, new BigDecimal(row.get("seatscale").asText()));
                    pstmt.setString(7, row.get("telno").asText());
                    pstmt.setString(8, row.get("relateurl").asText());
                    pstmt.setString(9, row.get("adres").asText());
                    pstmt.setBigDecimal(10, new BigDecimal(row.get("la").asText()));
                    pstmt.setBigDecimal(11, new BigDecimal(row.get("lo").asText()));

                    pstmt.addBatch();
                    count++;
                }
            }
            pstmt.executeBatch();

            Optional<Integer> updtCount = Utils.getUpdtCount(tableName);
            if (!updtCount.isPresent()) {
                throw new Exception("Failed to get updtCount");
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
