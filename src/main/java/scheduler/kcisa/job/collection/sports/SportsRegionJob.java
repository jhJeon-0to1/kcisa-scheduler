package scheduler.kcisa.job.collection.sports;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.jetbrains.annotations.NotNull;
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
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
public class SportsRegionJob extends QuartzJobBean {
    DataSource dataSource;
    SchedulerLogService schedulerLogService;
    String url = "http://data.prosports.or.kr/spectator/m0204/ajax/searchall";
    List<Code> codeList = new ArrayList<>();
    String tableName = "COLCT_SPORTS_VIEWNG_INFO";
    WebClient webClient = WebClient.builder().baseUrl("http://data.prosports.or.kr").build();
    Connection connection;

    @Autowired
    public SportsRegionJob(DataSource dataSource, SchedulerLogService schedulerLogService) throws SQLException {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;
        connection = dataSource.getConnection();

        codeList.add(new Code("LOC01", "서울", "11"));
        codeList.add(new Code("LOC02", "부산", "26"));
        codeList.add(new Code("LOC03", "대구", "27"));
        codeList.add(new Code("LOC04", "인천", "28"));
        codeList.add(new Code("LOC05", "광주", "29"));
        codeList.add(new Code("LOC06", "대전", "30"));
        codeList.add(new Code("LOC07", "울산", "31"));
        codeList.add(new Code("LOC09", "경기", "41"));
        codeList.add(new Code("LOC10", "강원", "51"));
        codeList.add(new Code("LOC11", "충북", "43"));
        codeList.add(new Code("LOC12", "충남", "44"));
        codeList.add(new Code("LOC13", "전북", "45"));
        codeList.add(new Code("LOC14", "전남", "46"));
        codeList.add(new Code("LOC15", "경북", "47"));
        codeList.add(new Code("LOC16", "경남", "48"));
        codeList.add(new Code("LOC17", "제주", "50"));
    }

    @Override
    protected void executeInternal(org.quartz.JobExecutionContext context) {
        int count = 0;
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();

        schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

        try {
            LocalDate stdDate = LocalDate.now().minusDays(2);
            String date = stdDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            String year = stdDate.format(DateTimeFormatter.ofPattern("yyyy"));
            String month = stdDate.format(DateTimeFormatter.ofPattern("MM"));
            String day = stdDate.format(DateTimeFormatter.ofPattern("dd"));

            String inputQuery = "INSERT analysis_etl.COLCT_SPORTS_VIEWNG_INFO (BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD, CTPRVN_NM, KLEA_VIEWING_NMPR_CO, KBO_VIEWING_NMPR_CO, KBL_VIEWING_NMPR_CO, WKBL_VIEWING_NMPR_CO, KOVO_VIEWING_NMPR_CO, SPORTS_VIEWING_NMPR_CO, KLEA_MATCH_CO, KBO_MATCH_CO, KBL_MATCH_CO, WKBL_MATCH_CO, KOVO_MATCH_CO, SPORTS_MATCH_CO, COLCT_DE) VALUE (?, ?, ?, ?, ?, (SELECT CTPRVN_NM FROM CTPRVN_INFO AS C WHERE C.CTPRVN_CD = ?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_FORMAT(NOW(), '%Y%m%d')) ON DUPLICATE KEY UPDATE BASE_DE = VALUES(BASE_DE) , BASE_YEAR = VALUES(BASE_YEAR) , BASE_MT = VALUES(BASE_MT) , BASE_DAY = VALUES(BASE_DAY) , CTPRVN_CD = VALUES(CTPRVN_CD) , KLEA_VIEWING_NMPR_CO = VALUES(KLEA_VIEWING_NMPR_CO) , KBO_VIEWING_NMPR_CO = VALUES(KBO_VIEWING_NMPR_CO) , KBL_VIEWING_NMPR_CO = VALUES(KBL_VIEWING_NMPR_CO) , WKBL_VIEWING_NMPR_CO = VALUES(WKBL_VIEWING_NMPR_CO) , KOVO_VIEWING_NMPR_CO = VALUES(KOVO_VIEWING_NMPR_CO) , SPORTS_VIEWING_NMPR_CO = VALUES(SPORTS_VIEWING_NMPR_CO) , KLEA_MATCH_CO = VALUES(KLEA_MATCH_CO) , KBO_MATCH_CO = VALUES(KBO_MATCH_CO) , KBL_MATCH_CO = VALUES(KBL_MATCH_CO) , WKBL_MATCH_CO = VALUES(WKBL_MATCH_CO) , KOVO_MATCH_CO = VALUES(KOVO_MATCH_CO) , SPORTS_MATCH_CO = VALUES(SPORTS_MATCH_CO), UPDT_DE = DATE_FORMAT(NOW(), '%Y%m%d')";

            PreparedStatement pstmt = connection.prepareStatement(inputQuery);

            for (Code code : codeList) {
                String regionCode = code.region;

                String codeId = code.code;

                JsonNodeFactory nodeFactory = new JsonNodeFactory(false);
                ObjectNode bodyData = new ObjectNode(nodeFactory);
                bodyData.set("agency", new TextNode("ALL"));
                bodyData.set("club", new TextNode("ALL"));
                bodyData.set("club_type", new TextNode("ALL"));
                bodyData.set("game_year", new TextNode(year));
                bodyData.set("game_month", new TextNode(month));
                bodyData.set("game_week", new TextNode("ALL"));
                bodyData.set("game_day", new TextNode(day));
                bodyData.set("league", new TextNode("ALL"));
                bodyData.set("local_code", new TextNode(codeId));
                bodyData.set("season", new TextNode("ALL"));
                bodyData.set("stadium", new TextNode("ALL"));
                bodyData.set("pageSize", new TextNode("30"));
                bodyData.set("startRow", new TextNode("-30"));

                JsonNode response = webClient.post().uri(url).contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(bodyData).retrieve().bodyToMono(JsonNode.class).block();

                if (response == null) {
                    schedulerLogService.create(
                            new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, "해당일자 데이터가 없습니다."));
                    return;
                }

                pstmt.setString(1, date);
                pstmt.setString(2, year);
                pstmt.setString(3, month);
                pstmt.setString(4, day);
                pstmt.setString(5, regionCode);
                pstmt.setString(6, regionCode);
                pstmt.setBigDecimal(7, new BigDecimal(response.get("total1").asText()));
                pstmt.setBigDecimal(8, new BigDecimal(response.get("total2").asText()));
                pstmt.setBigDecimal(9, new BigDecimal(response.get("total3").asText()));
                pstmt.setBigDecimal(10, new BigDecimal(response.get("total4").asText()));
                pstmt.setBigDecimal(11, new BigDecimal(response.get("total5").asText()));
                pstmt.setBigDecimal(12, new BigDecimal(response.get("total6").asText()));
                pstmt.setBigDecimal(13, new BigDecimal(response.get("gamecnt1").asText()));
                pstmt.setBigDecimal(14, new BigDecimal(response.get("gamecnt2").asText()));
                pstmt.setBigDecimal(15, new BigDecimal(response.get("gamecnt3").asText()));
                pstmt.setBigDecimal(16, new BigDecimal(response.get("gamecnt4").asText()));
                pstmt.setBigDecimal(17, new BigDecimal(response.get("gamecnt5").asText()));
                pstmt.setBigDecimal(18, new BigDecimal(response.get("gamecnt6").asText()));

                count++;
                pstmt.addBatch();
            }

            pstmt.executeBatch();

            System.out.println("스포츠 지역별 관중 수집 완료");

            Optional<Integer> updt_count = Utils.getUpdtCount(tableName);
            if (!updt_count.isPresent()) {
                throw new CustomException("002", "updt_count error");
            }
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count,
                    count - updt_count.get(), updt_count.get()));

        } catch (Exception e) {
            System.out.println("스포츠 지역별 관중 수집 실패");
            e.printStackTrace();
            schedulerLogService
                    .create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        }
    }

    private static class Code {
        String code;
        String name;
        String region;

        public Code(String code, String name, String region) {
            this.code = code;
            this.name = name;
            this.region = region;
        }
    }
}
