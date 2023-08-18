import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.test.model.SchedulerStatus;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class CTPRVNTest {
    private static final String started = SchedulerStatus.STARTED.name();
    private static final String success = SchedulerStatus.SUCCESS.name();
    private static final String failed = SchedulerStatus.FAILED.name();

    private static final LocalDate startDate = LocalDate.of(2023, 6, 1);
    private static final LocalDate endDate = LocalDate.now();

    public static String getCode(String name) {
        switch (name) {
            case "서울특별시":
                return "11";
            case "인천광역시":
                return "28";
            case "경기도":
                return "41";
            case "강원도":
            case "강원특별자치도":
                return "51";
            case "충청북도":
                return "43";
            case "충청남도":
                return "44";
            case "대전광역시":
                return "30";
            case "세종특별자치시":
                return "36";
            case "전라북도":
                return "45";
            case "전라남도":
                return "46";
            case "광주광역시":
                return "29";
            case "경상북도":
                return "47";
            case "경상남도":
                return "48";
            case "대구광역시":
                return "27";
            case "울산광역시":
                return "31";
            case "부산광역시":
                return "26";
            case "제주특별자치도":
                return "50";
            default:
                return null;
        }
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "rootroot");
        try {
            String sql = "INSERT INTO test.scheduler_log (group_name, job_name, status) VALUES (?,?,?)";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, "test");
            pstmt.setString(2, "시도 인구");
            pstmt.setString(3, started);

            pstmt.executeUpdate();
            WebClient webClient = WebClient.builder().baseUrl("https://jumin.mois.go.kr/statMonth.do").build();
            String url = "https://jumin.mois.go.kr/statMonth.do";
            int count = 0;

            String insertQuery = "INSERT INTO kcisa.popltn_info (base_year, base_month, CTPRVN_CD, ctprvn_nm, popltn_co) VALUES (?, ?, ?, (SELECT ctprvn_info.CTPRVN_NM FROM kcisa.ctprvn_info WHERE ctprvn_info.CTPRVN_CD = ?), ?) ON DUPLICATE KEY UPDATE popltn_co = ?";

            pstmt = conn.prepareStatement(insertQuery);
            for (LocalDate date = startDate; date.isBefore(endDate); date = date.plusMonths(1)) {
                String year = date.format(DateTimeFormatter.ofPattern("yyyy"));
                String month = date.format(DateTimeFormatter.ofPattern("MM"));

                String formData = "searchYearMonth=month&searchYearStart=" + year + "&searchMonthStart=" + month + "&searchYearEnd=" + year + "&searchMonthEnd=" + month + "&sltOrgType=1&sltOrgLvl1=A&sltOrgLvl2=A&generation=generation";
                String htmlResponse = webClient.post().uri(url).body(BodyInserters.fromValue(formData)).header("Content-Type", "application/x-www-form-urlencoded").retrieve().bodyToMono(String.class).block();

                if (htmlResponse == null) {
                    System.out.println(year + month + "htmlResponse is null");
                    continue;
                }

                Document doc = Jsoup.parse(htmlResponse);
                Element tbody = doc.selectFirst("div.section3 tbody");
                if (tbody == null) {
                    System.out.println(year + month + "tbody is null");
                    continue;
                }
                List<Element> rows = tbody.select("tr");

                for (Element row : rows) {
                    List<Element> cells = row.select("td");
                    String name = cells.get(1).text();
                    String code = getCode(name);
                    if (code == null) {
                        System.out.println(name + "code is null");
                        continue;
                    }
                    String population = cells.get(2).text().replace(",", "");

                    pstmt.setString(1, year);
                    pstmt.setString(2, month);
                    pstmt.setString(3, code);
                    pstmt.setString(4, code);
                    pstmt.setBigDecimal(5, new BigDecimal(population));
                    pstmt.setBigDecimal(6, new BigDecimal(population));
                    if (year.equals("2023") && month.equals("07")) {
                        System.out.println(pstmt);
                    }

                    pstmt.addBatch();
                    count++;
                }

            }
            pstmt.executeBatch();

            String sql2 = "INSERT test.scheduler_log (group_name, job_name, status, item_count) VALUES ('test', '시도 인구', ?, ?)";
            pstmt = conn.prepareStatement(sql2);
            pstmt.setString(1, success);
            pstmt.setInt(2, count);
            pstmt.executeUpdate();

            System.out.println("success");

            pstmt.close();
        } catch (Exception e) {
            String failedQuery = "INSERT test.scheduler_log (group_name, job_name, status, message) VALUES ('test', '시도 인구', ?, ?)";
            PreparedStatement pstmt = conn.prepareStatement(failedQuery);
            pstmt.setString(1, failed);
            pstmt.setString(2, e.getMessage());
            pstmt.executeUpdate();

            e.printStackTrace();
        }
    }
}
