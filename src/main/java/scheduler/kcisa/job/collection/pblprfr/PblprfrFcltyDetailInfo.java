package scheduler.kcisa.job.collection.pblprfr;

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
import scheduler.kcisa.model.flag.collection.MonthlyCollectionFlag;
import scheduler.kcisa.service.flag.collection.MonthlyCollectionFlagService;
import scheduler.kcisa.utils.JobUtils;
import scheduler.kcisa.utils.Utils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class PblprfrFcltyDetailInfo extends QuartzJobBean {
    MonthlyCollectionFlagService flagService;
    @Value("${kopis.api.key}")
    String key;
    WebClient webClient = WebClient.builder().baseUrl("http://kopis.or.kr/openApi/restful/prfplc").build();

    @Autowired
    public PblprfrFcltyDetailInfo(MonthlyCollectionFlagService flagService) {
        this.flagService = flagService;
    }

    protected void checkFcltyCollect() throws Exception {
        MonthlyCollectionFlag flag = flagService.findByDateAndTableName(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM")), "colct_pblprfr_fclty_info");
        if (flag == null) {
            throw new Exception("시설정보가 업데이트되지 않았습니다.");
        }
    }

    protected Boolean checkAlreadyCollect() {
        String date = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));
        MonthlyCollectionFlag flag = flagService.findByDateAndTableName(date, "colct_pblprfr_fclty_detail_info");
        return flag != null;
    }

    protected ArrayList<String> getFcltyId(String date, Connection connection) throws Exception {
        String selectIDQuery = "SELECT PBLPRFR_FCLTY_ID FROM colct_pblprfr_fclty_info WHERE COLCT_YM = ?";

        try (PreparedStatement selectIDPstmt = connection.prepareStatement(selectIDQuery)) {
            selectIDPstmt.setString(1, date);

            ResultSet rs = selectIDPstmt.executeQuery();
            ArrayList<String> idList = new ArrayList<>();
            while (rs.next()) {
                idList.add(rs.getString("PBLPRFR_FCLTY_ID"));
            }
            selectIDPstmt.close();

            return idList;
        }
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();
        String tableName = "colct_pblprfr_fclty_detail_info";
        XmlMapper xmlMapper = new XmlMapper();
        String date = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));

        AtomicInteger count = new AtomicInteger();
        Boolean isAlreadyCollect = checkAlreadyCollect();
        if (isAlreadyCollect) {
            System.out.println("시설 상세 정보가 이미 업데이트 되었습니다.");

            return;
        }

        JobUtils.executeJob(context, tableName, jobData -> {
            Connection connection = jobData.conn;
            checkFcltyCollect();

            ArrayList<String> idList = getFcltyId(date, connection);


            String insertQuery = "INSERT INTO colct_pblprfr_fclty_detail_info (PBLPRFR_FCLTY_ID,PBLPRFR_FCLTY_NM,PRFPLC_CO,FCLTY_CHARTR,OPNNG_YEAR,FCLTY_SEAT_CO,FCLTY_TEL_NO,FCLTY_URL,FCLTY_ADDR,FCLTY_LA,FLCTY_LO,COLCT_YM) VALUES (?,?,?,?,?,?,?,?,?,?,?,DATE_FORMAT(NOW(), '%Y%m')) ON DUPLICATE KEY UPDATE PBLPRFR_FCLTY_NM=VALUES(PBLPRFR_FCLTY_NM),PRFPLC_CO=VALUES(PRFPLC_CO),FCLTY_CHARTR=VALUES(FCLTY_CHARTR),OPNNG_YEAR=VALUES(OPNNG_YEAR),FCLTY_SEAT_CO=VALUES(FCLTY_SEAT_CO),FCLTY_TEL_NO=VALUES(FCLTY_TEL_NO),FCLTY_URL=VALUES(FCLTY_URL),FCLTY_ADDR=VALUES(FCLTY_ADDR),FCLTY_LA=VALUES(FCLTY_LA),FLCTY_LO=VALUES(FLCTY_LO),UPDT_YM=DATE_FORMAT(NOW(), '%Y%m')";

            try (PreparedStatement pstmt = connection.prepareStatement(insertQuery)) {
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
                        count.getAndIncrement();
                    }
                }
                pstmt.executeBatch();

                Optional<Integer> updtCount = Utils.getUpdtCount(tableName);
                if (!updtCount.isPresent()) {
                    throw new Exception("Failed to get updtCount");
                }

                jobData.logService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count.get(),
                        count.get() - updtCount.get(), updtCount.get()));

                flagService.create(new MonthlyCollectionFlag(date, tableName, true));
            }
        });


    }
}
