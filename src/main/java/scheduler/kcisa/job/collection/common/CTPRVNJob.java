package scheduler.kcisa.job.collection.common;

import org.jetbrains.annotations.NotNull;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.service.SchedulerLogService;
import scheduler.kcisa.utils.Utils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

@Component
public class CTPRVNJob extends QuartzJobBean {
    private final String tableName = "popltn_info";
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
        return switch (name) {
            case "서울특별시" -> "11";
            case "인천광역시" -> "28";
            case "경기도" -> "41";
            case "강원도", "강원특별자치도" -> "51";
            case "충청북도" -> "43";
            case "충청남도" -> "44";
            case "대전광역시" -> "30";
            case "세종특별자치시" -> "36";
            case "전라북도" -> "45";
            case "전라남도" -> "46";
            case "광주광역시" -> "29";
            case "경상북도" -> "47";
            case "경상남도" -> "48";
            case "대구광역시" -> "27";
            case "울산광역시" -> "31";
            case "부산광역시" -> "26";
            case "제주특별자치도" -> "50";
            default -> null;
        };
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

            String insertQuery = "INSERT INTO kcisa.popltn_info (base_year, base_month, CTPRVN_CD, ctprvn_nm, popltn_co) VALUES (?, ?, ?, (SELECT CTPRVN_INFO.CTPRVN_NM FROM kcisa.CTPRVN_INFO WHERE CTPRVN_INFO.CTPRVN_CD = ?), ?) ON DUPLICATE KEY UPDATE popltn_co = VALUES(popltn_co), updt_dt = NOW()";

            PreparedStatement pstmt = connection.prepareStatement(insertQuery);

            for (LocalDate date = startDate; date.isBefore(endDate.plusMonths(1)); date = date.plusMonths(1)) {
                BigDecimal total = BigDecimal.valueOf(0);
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
                    total = total.add(new BigDecimal(population));

                    pstmt.addBatch();
                    count++;
                }

                pstmt.setString(1, year);
                pstmt.setString(2, month);
                pstmt.setString(3, "00");
                pstmt.setString(4, "00");
                pstmt.setBigDecimal(5, total);

                pstmt.addBatch();
                count++;
            }

            pstmt.executeBatch();
            connection.close();

            Optional<Integer> updt_cnt = Utils.getUpdtCount(tableName);
            if (!updt_cnt.isPresent()) {
                throw new Exception("updt_cnt is empty");
            }
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count, count - updt_cnt.get(), updt_cnt.get()));

            System.out.println("시도 인구 추가 완료");
        } catch (Exception e) {
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
            System.out.println("시도 인구 추가 실패");
            e.printStackTrace();
        }
    }
}
