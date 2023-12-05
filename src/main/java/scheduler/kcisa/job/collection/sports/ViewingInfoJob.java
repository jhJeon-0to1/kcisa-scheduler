package scheduler.kcisa.job.collection.sports;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.model.flag.collection.DailyCollectionFlag;
import scheduler.kcisa.service.SchedulerLogService;
import scheduler.kcisa.service.flag.collection.DailyCollectionFlagService;
import scheduler.kcisa.utils.CustomException;
import scheduler.kcisa.utils.JobUtils;
import scheduler.kcisa.utils.Utils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ViewingInfoJob extends QuartzJobBean {
    DailyCollectionFlagService flagService;
    String url = "http://data.prosports.or.kr/spectator/m0204/ajax/searchall";
    List<Code> codeList = new ArrayList<>();
    String tableName = "colct_sports_viewng_info";
    WebClient webClient = WebClient.builder().baseUrl("http://data.prosports.or.kr").build();

    @Autowired
    public ViewingInfoJob(DailyCollectionFlagService flagService) {
        this.flagService = flagService;

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
        LocalDate stdDate = LocalDate.now().minusDays(2);
        String date = stdDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String year = stdDate.format(DateTimeFormatter.ofPattern("yyyy"));
        String month = stdDate.format(DateTimeFormatter.ofPattern("MM"));
        String day = stdDate.format(DateTimeFormatter.ofPattern("dd"));

        AtomicInteger count = new AtomicInteger();

        JobUtils.executeJob(context, tableName, jobData -> {
            Connection connection = jobData.conn;
            String groupName = jobData.groupName;
            String jobName = jobData.jobName;
            SchedulerLogService schedulerLogService = (SchedulerLogService) jobData.logService;

            String inputQuery = Utils.getSQLString("src/main/resources/sql/collection/sports/ViewngInfoJob.sql");

            try (PreparedStatement pstmt = connection.prepareStatement(inputQuery)) {
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

                    count.getAndIncrement();
                    pstmt.addBatch();
                }

                pstmt.executeBatch();

                System.out.println("스포츠 지역별 관중 수집 완료");

                Optional<Integer> updt_count = Utils.getUpdtCount(tableName);
                if (!updt_count.isPresent()) {
                    throw new CustomException("002", "updt_count error");
                }
                schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count.get(), count.get() - updt_count.get(), updt_count.get()));

                flagService.create(new DailyCollectionFlag(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")), tableName, true));
            }
        });
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
