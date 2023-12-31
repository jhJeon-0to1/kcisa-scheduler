package scheduler.kcisa.job.collection.sports;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
import java.util.Objects;
import java.util.Optional;

@Component
public class MatchInfoJob extends QuartzJobBean {
    String url = "http://data.prosports.or.kr/spectator/m0201/ajax/search";
    String tableName = "colct_sports_match_info";
    WebClient webClient = WebClient.builder().baseUrl("http://data.prosports.or.kr").build();
    DailyCollectionFlagService flagService;

    @Autowired
    public MatchInfoJob(DailyCollectionFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(org.quartz.JobExecutionContext context) {
        System.out.println("스포츠 일별 경기 수집 시작");

        JobUtils.executeJob(context, tableName, jobData -> {
            String groupName = jobData.groupName;
            String jobName = jobData.jobName;
            SchedulerLogService schedulerLogService = (SchedulerLogService) jobData.logService;
            Connection connection = jobData.conn;

            int count = 0;

            LocalDate yesterday = LocalDate.now().minusDays(2);
            String date = yesterday.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

            String inputQuery = Utils.getSQLString("src/main/resources/sql/collection/sports/MatchInfoJob.sql");

            try (PreparedStatement pstmt = connection.prepareStatement(inputQuery);) {
                int startRow = 0;
                while (true) {
                    JsonNodeFactory nodeFactory = new JsonNodeFactory(false);
                    ObjectNode bodyData = new ObjectNode(nodeFactory);
                    bodyData.set("agency", new ArrayNode(nodeFactory).add("ALL"));
                    bodyData.set("season", new ArrayNode(nodeFactory).add("ALL"));
                    bodyData.set("season_type", new TextNode("ALL"));
                    bodyData.set("club_type", new TextNode("ALL"));
                    bodyData.set("game_week", new ArrayNode(null));
                    bodyData.set("str_date", new TextNode(date));
                    bodyData.set("end_date", new TextNode(date));
                    bodyData.set("pageSize", new TextNode("100"));
                    bodyData.set("startRowNo", new TextNode(String.valueOf(startRow)));

                    ArrayNode response = webClient.post().uri(url).contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(bodyData).retrieve().bodyToMono(ArrayNode.class).block();

                    if (Objects.requireNonNull(response).isEmpty()) {
                        break;
                    }
                    startRow += 100;
                    for (JsonNode row : response) {
                        pstmt.setString(1, row.get("game_seq").asText().trim());
                        pstmt.setString(2, row.get("year").asText().trim() + row.get("month").asText().trim()
                                + row.get("day").asText().trim());
                        pstmt.setString(3, row.get("year").asText().trim());
                        pstmt.setString(4, row.get("month").asText().trim());
                        pstmt.setString(5, row.get("day").asText().trim());
                        pstmt.setString(6, row.get("agency_name").asText().trim());
                        pstmt.setString(7, row.get("league_nm").asText().trim());
                        pstmt.setString(8, row.get("h_club_name").asText().trim());
                        pstmt.setString(9, row.get("a_club_name").asText().trim());
                        pstmt.setString(10, row.get("stadium_nm").asText().trim());
                        pstmt.setBigDecimal(11, new BigDecimal(row.get("spec_num").asText()));

                        pstmt.addBatch();

                        count++;
                    }
                }

                pstmt.executeBatch();

                Optional<Integer> updt_count = Utils.getUpdtCount(tableName);
                if (!updt_count.isPresent()) {
                    throw new CustomException("002", "getUpdtCount error");
                }

                System.out.println("스포츠 일별 경기 수집 완료");
                schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count, count - updt_count.get(), updt_count.get()));

                flagService.create(new DailyCollectionFlag(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")), tableName, true));
            }
        });

    }
}
