package scheduler.test.job.common;

import org.jetbrains.annotations.NotNull;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.test.model.SchedulerLog;
import scheduler.test.model.SchedulerStatus;
import scheduler.test.service.SchedulerLogService;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Component
public class CTPRVNJob extends QuartzJobBean {
    private final String tableName = "ctprvn_info";
    private final DataSource dataSource;
    private final SchedulerLogService schedulerLogService;
    WebClient webClient = WebClient.builder().baseUrl("https://jumin.mois.go.kr/statMonth.do").build();
    String url = "https://jumin.mois.go.kr/statMonth.do";

    @Autowired
    public CTPRVNJob(DataSource dataSource, SchedulerLogService schedulerLogService) {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;
    }

    private static String getCode(String name) {
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

    @Override
    protected void executeInternal(@NotNull org.quartz.JobExecutionContext jobExecutionContext) throws org.quartz.JobExecutionException {
        String groupName = jobExecutionContext.getJobDetail().getKey().getGroup();
        String jobName = jobExecutionContext.getJobDetail().getKey().getName();

        try {
            int count = 0;
            final LocalDate endDate = LocalDate.now().minusMonths(1).withDayOfMonth(1);
            final LocalDate startDate = endDate.minusMonths(1).withDayOfMonth(1);

            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            Connection connection = dataSource.getConnection();

            String insertQuery = "INSERT INTO kcisa.popltn_info (base_year, base_month, CTPRVN_CD, ctprvn_nm, popltn_co) VALUES (?, ?, ?, (SELECT ctprvn_info.CTPRVN_NM FROM kcisa.ctprvn_info WHERE ctprvn_info.CTPRVN_CD = ?), ?) ON DUPLICATE KEY UPDATE popltn_co = ?";

            PreparedStatement pstmt = connection.prepareStatement(insertQuery);

            for (LocalDate date = startDate; date.isBefore(endDate.plusMonths(1)); date = date.plusMonths(1)) {
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

                    pstmt.addBatch();
                    count++;
                }
            }
            pstmt.executeBatch();
            connection.close();

            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count));

            System.out.println("시도 인구 추가 완료");
        } catch (Exception e) {
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
            System.out.println("시도 인구 추가 실패");
            e.printStackTrace();
        }
    }
}
