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
import scheduler.kcisa.model.flag.collection.MonthlyCollectionFlag;
import scheduler.kcisa.service.SchedulerLogService;
import scheduler.kcisa.service.flag.collection.MonthlyCollectionFlagService;
import scheduler.kcisa.utils.JobUtils;
import scheduler.kcisa.utils.ScheduleInterval;
import scheduler.kcisa.utils.Utils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Component
public class CtprvnAcctoPopltnInfo extends QuartzJobBean {
    private final DataSource dataSource;
    private final SchedulerLogService schedulerLogService;
    private final MonthlyCollectionFlagService flagService;
    WebClient webClient = WebClient.builder().baseUrl("https://jumin.mois.go.kr/statMonth.do").build();
    String url = "https://jumin.mois.go.kr/statMonth.do";

    @Autowired
    public CtprvnAcctoPopltnInfo(DataSource dataSource, SchedulerLogService schedulerLogService, MonthlyCollectionFlagService monthlyCollectionFlagService) {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;
        this.flagService = monthlyCollectionFlagService;
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
            case "전국":
                return "00";
            default:
                return null;
        }
    }

    @Override
    protected void executeInternal(@NotNull org.quartz.JobExecutionContext jobExecutionContext)
            throws org.quartz.JobExecutionException {
        String groupName = jobExecutionContext.getJobDetail().getKey().getGroup();
        String jobName = jobExecutionContext.getJobDetail().getKey().getName();

        String tableName = "ctprvn_accto_popltn_info";
        List<String> checkList = Arrays.asList(tableName);

        try (Connection connection = dataSource.getConnection()) {
            String existTable = JobUtils.checkCollectFlag(checkList, LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")), ScheduleInterval.DAILY);
            if (existTable != null) {
                System.out.println(existTable + " 테이블은 이미 수집 완료 되었습니다.");
                return;
            }

            int count = 0;
            final LocalDate endDate = LocalDate.now().minusMonths(1).withDayOfMonth(1);
            final LocalDate startDate = endDate.minusMonths(1).withDayOfMonth(1);

            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            String insertQuery = "INSERT INTO analysis_etl.ctprvn_accto_popltn_info (BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM, POPLTN_CO, COLCT_DE) VALUES (?, ?, ?, ?, (SELECT C.CTPRVN_NM FROM ctprvn_info AS C WHERE C.CTPRVN_CD = ?), ?, DATE_FORMAT(NOW(), '%Y%m%d')) ON DUPLICATE KEY UPDATE popltn_co = VALUES(popltn_co), UPDT_DE = DATE_FORMAT(NOW(), '%Y%m%d')";

            try (PreparedStatement pstmt = connection.prepareStatement(insertQuery);) {
                for (LocalDate date = startDate; date.isBefore(endDate.plusMonths(1)); date = date.plusMonths(1)) {
                    String year = date.format(DateTimeFormatter.ofPattern("yyyy"));
                    String month = date.format(DateTimeFormatter.ofPattern("MM"));

                    String formData = "searchYearMonth=month&searchYearStart=" + year + "&searchMonthStart=" + month
                            + "&searchYearEnd=" + year + "&searchMonthEnd=" + month
                            + "&sltOrgType=1&sltOrgLvl1=A&sltOrgLvl2=A&generation=generation";
                    String htmlResponse = webClient.post().uri(url).body(BodyInserters.fromValue(formData))
                            .header("Content-Type", "application/x-www-form-urlencoded").retrieve().bodyToMono(String.class)
                            .block();

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

                        pstmt.setString(1, year + month);
                        pstmt.setString(2, year);
                        pstmt.setString(3, month);
                        pstmt.setString(4, code);
                        pstmt.setString(5, code);
                        pstmt.setBigDecimal(6, new BigDecimal(population));

                        pstmt.addBatch();
                        count++;
                    }
                }

                pstmt.executeBatch();

                Optional<Integer> updt_cnt = Utils.getUpdtCount(tableName);
                if (!updt_cnt.isPresent()) {
                    throw new Exception("updt_cnt is empty");
                }
                schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count,
                        count - updt_cnt.get(), updt_cnt.get()));

                String date = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));
                flagService.create(new MonthlyCollectionFlag(date, tableName, true));

                System.out.println(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + " 시도 인구 추가 완료");
            }
        } catch (Exception e) {
            schedulerLogService
                    .create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));

            System.out.println(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + " 시도 인구 추가 실패");
            e.printStackTrace();
        }
    }
}
