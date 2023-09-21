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
import scheduler.kcisa.service.SchedulerLogService;
import scheduler.kcisa.utils.Utils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;

@Component
public class SportsDailyJob extends QuartzJobBean {
    DataSource dataSource;
    SchedulerLogService schedulerLogService;
    String url = "http://data.prosports.or.kr/spectator/m0201/ajax/search";
    String tableName = "sports_일별경기";
    WebClient webClient = WebClient.builder().baseUrl("http://data.prosports.or.kr").build();
    Connection connection;

    @Autowired
    public SportsDailyJob(DataSource dataSource, SchedulerLogService schedulerLogService) throws Exception {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;

        connection = dataSource.getConnection();
    }

    @Override
    protected void executeInternal(org.quartz.JobExecutionContext context) {
        System.out.println("스포츠 일별 경기 수집 시작");
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();
        try {
            int count = 0;

            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            LocalDate yesterday = LocalDate.now().minusDays(2);
            String date = yesterday.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

            String inputQuery = "INSERT kcisa.sports_일별경기 (game_seq, date, year, month, day, agency_name, stadium_nm, home_name, away_name, league_nm, spec_num) VALUE (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE date = VALUES(date) , year = VALUES(year) , month = VALUES(month) , day = VALUES(day) , agency_name = VALUES(agency_name) , stadium_nm = VALUES(stadium_nm) , home_name = VALUES(home_name) , away_name = VALUES(away_name) , league_nm = VALUES(league_nm) , spec_num = VALUES(spec_num), updt_dt=NOW()";
            PreparedStatement pstmt = connection.prepareStatement(inputQuery);

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

                ArrayNode response = webClient.post().uri(url).contentType(MediaType.APPLICATION_JSON).bodyValue(bodyData).retrieve().bodyToMono(ArrayNode.class).block();

                if (Objects.requireNonNull(response).isEmpty()) {
                    break;
                }
                startRow += 100;
                for (JsonNode row : response) {
                    pstmt.setString(1, row.get("game_seq").asText().trim());
                    pstmt.setString(2, row.get("year").asText().trim() + row.get("month").asText().trim() + row.get("day").asText().trim());
                    pstmt.setString(3, row.get("year").asText().trim());
                    pstmt.setString(4, row.get("month").asText().trim());
                    pstmt.setString(5, row.get("day").asText().trim());
                    pstmt.setString(6, row.get("agency_name").asText().trim());
                    pstmt.setString(7, row.get("stadium_nm").asText().trim());
                    pstmt.setString(8, row.get("h_club_name").asText().trim());
                    pstmt.setString(9, row.get("a_club_name").asText().trim());
                    pstmt.setString(10, row.get("league_nm").asText().trim());
                    pstmt.setBigDecimal(11, row.get("spec_num").decimalValue());

                    pstmt.addBatch();

                    count++;
                }
            }

            pstmt.executeBatch();

            Optional<Integer> updt_count = Utils.getUpdtCount(tableName);
            if (!updt_count.isPresent()) {
                throw new Exception("getUpdtCount error");
            }

            System.out.println("스포츠 일별 경기 수집 완료");
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count, count - updt_count.get(), updt_count.get()));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("스포츠 일별 경기 수집 실패");
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        }
    }
}
