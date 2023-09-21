import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.kcisa.service.SchedulerLogService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class SportsDailyTest {
    private static final String url = "http://data.prosports.or.kr/spectator/m0201/ajax/search";
    private static final WebClient webClient = WebClient.builder().baseUrl("http://data.prosports.or.kr").build();
    static Connection connection;
    DataSource dataSource;
    SchedulerLogService schedulerLogService;
    String tableName = "sports_일별관중";

    public static void main(String[] args) {
        System.out.println("스포츠 일별 경기 수집 시작");

        try {
            LocalDate yesterday = LocalDate.now().minusDays(1);
            String date = yesterday.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

//            String inputQuery = "INSERT kcisa.sports_일별경기 (date, year, month, day, agency_name, stadium_nm, home_name, away_name, league_nm, spec_num) VALUE (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
//            PreparedStatement pstmt = connection.prepareStatement(inputQuery);

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
                    System.out.println(row);
//                    pstmt.setString(1, date);
//                    pstmt.setString(2, row.get("year").asText());
//                    pstmt.setString(3, row.get("month").asText());
//                    pstmt.setString(4, row.get("day").asText());
//                    pstmt.setString(5, row.get("agency_name").asText());
//                    pstmt.setString(6, row.get("stadium_nm").asText());
//                    pstmt.setString(7, row.get("h_club_name").asText());
//                    pstmt.setString(8, row.get("a_club_name").asText());
//                    pstmt.setString(9, row.get("league_nm").asText());
//                    pstmt.setBigDecimal(10, row.get("spec_num").decimalValue());
//
//                    pstmt.addBatch();

//                    count++;
                }
            }

//            pstmt.executeBatch();

            System.out.println("스포츠 일별 경기 수집 완료");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("스포츠 일별 경기 수집 실패");
        }
    }
}
