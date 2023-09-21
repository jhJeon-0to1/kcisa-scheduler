import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import scheduler.kcisa.model.SchedulerStatus;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;


public class KopisTest {
    private static final List<String> sidos = Arrays.asList("^11", "^28", "^41", "^30", "^36", "^44", "^43", "^51|^42", "^27", "^26", "^31", "^48", "^47", "^29", "^46", "^45", "^50");
    private static final List<String> ctprvn = Arrays.asList("11", "28", "41", "30", "36", "44", "43", "51", "27", "26", "31", "48", "47", "29", "46", "45", "50");
    private static final List<String> sido_names = Arrays.asList("서울시", "인천시", "경기도", "대전시", "세종시", "충청남도", "충청북도", "강원도", "대구시", "부산시", "울산시", "경상남도", "경상북도", "광주시", "전라남도", "전라북도", "제주도");
    private static final WebClient webClient = WebClient.builder().baseUrl("http://www.kopis.or.kr").build();
    private static final ObjectMapper objectMapper = new ObjectMapper();


    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "rootroot");

        LocalDate date = LocalDate.now().minusDays(2);
        String dateStr = date.format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));
        String year = String.valueOf(date.getYear());
        String month = String.valueOf(date.getMonthValue());
        String day = String.valueOf(date.getDayOfMonth());

        List<JsonNode> dataList = new ArrayList<>();
        String started = SchedulerStatus.STARTED.name();
        String sql = "INSERT INTO test.scheduler_log (group_name, job_name, status) VALUES (?,?,?)";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setString(1, "test");
        pstmt.setString(2, "test");
        pstmt.setString(3, started);

        pstmt.executeUpdate();

        for (int i = 0; i < sidos.size(); i++) {
            String sido = sidos.get(i);
            String sido_name = sido_names.get(i);
            System.out.println(sido_name);
            String ctprvn_cd = ctprvn.get(i);

            String url = "/por/stats/perfo/perfoStatsTotalList.json";

            Mono<JsonNode> response = webClient.get().uri(uriBuilder -> uriBuilder.path(url).queryParam("startDate", dateStr).queryParam("endDate", dateStr).queryParam("signgu_code", sido).build()).retrieve().bodyToMono(JsonNode.class);

            JsonNode jsonResponse = response.block();

            JsonNode rows = Objects.requireNonNull(jsonResponse).get("result");
            System.out.println(rows);
            if (rows != null && rows.isArray()) {
                for (JsonNode row : rows) {
                    ObjectNode newRow = objectMapper.createObjectNode();
                    String nowSido = row.get("signgu_codeNm").asText();
                    String nowGenreCode = row.get("genre_code").asText();

                    if (!Objects.equals(nowSido, "합계") && !Objects.equals(nowGenreCode, "null")) {
                        newRow.put("BASE_DE", dateStr.replace(".", ""));
                        newRow.put("BASE_YEAR", year);
                        newRow.put("BASE_MT", month);
                        newRow.put("BASE_DAY", day);
                        newRow.put("CTPRVN_CD", ctprvn_cd);
                        newRow.put("CTPRVN_NM", sido_name);
                        newRow.put("GENRE_CD", row.get("genre_code").asText());
                        newRow.put("GENRE_NM", row.get("genre_code_nm").asText());
                        newRow.put("PBLPRFR_RASNG_CUTIN_CO", row.get("data1").asText());
//                            newRow.put("PBLPRFR_RASNG_CUTIN_OCCU_RT", row.get("data2").asText());
                        newRow.put("PBLPRFR_CO", row.get("data3").asText());
//                            newRow.put("PBLPRFR_OCCU_RT", row.get("data4").asText());
                        newRow.put("PBLPRFR_STGNG_CO", row.get("data16").asText());
//                            newRow.put("PBLPRFR_STGNG_OCCU_RT", row.get("data17").asText());
                        newRow.put("PBLPRFR_SALES_PRICE", row.get("data5").asText());
//                            newRow.put("PBLPRFR_SALES_PRICE_RT", row.get("data6").asText());
                        newRow.put("PBLPRFR_VIEWNG_NMPR_CO", row.get("data7").asText());
//                            newRow.put("PBLPRFR_VIEWNG_NMPR_RT", row.get("data8").asText());

                        dataList.add(newRow);
                    }
                }


            } else {
                throw new Exception("result is null");
            }
        }

        String query = "INSERT INTO kcisa.pblprfr_viewing_info (BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD, CTPRVN_NM, GENRE_CD, GENRE_NM, PBLPRFR_RASNG_CUTIN_CO, PBLPRFR_RASNG_CUTIN_OCCU_RT, PBLPRFR_CO, PBLPRFR_OCCU_RT, PBLPRFR_STGNG_CO, PBLPRFR_STGNG_OCCU_RT, PBLPRFR_SALES_PRICE, PBLPRFR_SALES_PRICE_RT, PBLPRFR_VIEWNG_NMPR_CO, PBLPRFR_VIEWNG_NMPR_RT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE BASE_DE = ?";

        pstmt = conn.prepareStatement(query);

        for (JsonNode data : dataList) {
            pstmt.setString(1, data.get("BASE_DE").asText());
            pstmt.setString(2, data.get("BASE_YEAR").asText());
            pstmt.setString(3, data.get("BASE_MT").asText());
            pstmt.setString(4, data.get("BASE_DAY").asText());
            pstmt.setString(5, data.get("CTPRVN_CD").asText());
            pstmt.setString(6, data.get("CTPRVN_NM").asText());
            pstmt.setString(7, data.get("GENRE_CD").asText());
            pstmt.setString(8, data.get("GENRE_NM").asText());
            pstmt.setBigDecimal(9, new BigDecimal(data.get("PBLPRFR_RASNG_CUTIN_CO").asText()));
            pstmt.setBigDecimal(10, new BigDecimal(0));
            pstmt.setBigDecimal(11, new BigDecimal(data.get("PBLPRFR_CO").asText()));
            pstmt.setBigDecimal(12, new BigDecimal(0));
            pstmt.setBigDecimal(13, new BigDecimal(data.get("PBLPRFR_STGNG_CO").asText()));
            pstmt.setBigDecimal(14, new BigDecimal(0));
            pstmt.setBigDecimal(15, new BigDecimal(data.get("PBLPRFR_SALES_PRICE").asText()));
            pstmt.setBigDecimal(16, new BigDecimal(0));
            pstmt.setBigDecimal(17, new BigDecimal(data.get("PBLPRFR_VIEWNG_NMPR_CO").asText()));
            pstmt.setBigDecimal(18, new BigDecimal(0));
            pstmt.setString(19, data.get("BASE_DE").asText());
            pstmt.addBatch();
        }

        pstmt.executeBatch();

        String ended = SchedulerStatus.SUCCESS.name();
        String endQuery = "INSERT INTO test.scheduler_log (group_name, job_name, status, item_count) VALUES (?, ?, ?, ?)";
        pstmt = conn.prepareStatement(endQuery);
        pstmt.setString(1, "test");
        pstmt.setString(2, "test");
        pstmt.setString(3, ended);
        pstmt.setInt(4, dataList.size());

        pstmt.executeUpdate();

        System.out.println("end");
    }
}
