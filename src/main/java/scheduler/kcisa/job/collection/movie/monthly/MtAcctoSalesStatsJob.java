package scheduler.kcisa.job.collection.movie.monthly;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.quartz.JobExecutionContext;
import org.springframework.http.MediaType;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.model.flag.collection.MonthlyCollectionFlag;
import scheduler.kcisa.service.SchedulerLogService;
import scheduler.kcisa.service.flag.collection.MonthlyCollectionFlagService;
import scheduler.kcisa.utils.JobUtils;
import scheduler.kcisa.utils.Utils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class MtAcctoSalesStatsJob extends QuartzJobBean {
    MonthlyCollectionFlagService flagService;
    WebClient webClient = WebClient.builder().baseUrl("https://www.kobis.or.kr").build();
    String tableName = "colct_movie_mt_accto_sales_stats";

    public MtAcctoSalesStatsJob(MonthlyCollectionFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        LocalDate stdDate = LocalDate.now();
        String dateStr = stdDate.toString().replace("-", "");
        String year = dateStr.substring(0, 4);

        AtomicInteger count = new AtomicInteger();
        JobUtils.executeJob(context, tableName, jobData -> {
            String groupName = jobData.groupName;
            String jobName = jobData.jobName;
            Connection connection = jobData.conn;
            SchedulerLogService schedulerLogService = (SchedulerLogService) jobData.logService;

            String query = Utils.getSQLString("src/main/resources/sql/collection/movie/MtAcctoSalesStats.sql");
            try (PreparedStatement pstmt = connection.prepareStatement(query);) {
                String formData = "loadVal=0&searchType=search&selectYear=" + year;
                String response = webClient.post().uri("/kobis/business/stat/them/findMonthlyTotalList.do").contentType(MediaType.APPLICATION_FORM_URLENCODED).bodyValue(formData).retrieve().bodyToMono(String.class).block();

                if (response == null) {
                    schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, "response is null"));
                    return;
                }

                Document html = Jsoup.parse(response);
                List<Element> rows = html.select("tbody > tr");

                for (Element row : rows) {
                    List<Element> cells = row.select("td");
                    if (cells.get(0).text().equals("합계")) {
                        continue;
                    }
                    String date = cells.get(0).text().replace("-", "");
                    BigDecimal allOpening = new BigDecimal(cells.get(11).text().replace(",", ""));
                    BigDecimal allCount = new BigDecimal(cells.get(12).text().replace(",", ""));
                    BigDecimal allAmount = new BigDecimal(cells.get(13).text().replace(",", ""));
                    BigDecimal allPeople = new BigDecimal(cells.get(14).text().replace(",", ""));

                    pstmt.setString(1, date);
                    pstmt.setString(2, date.substring(0, 4));
                    pstmt.setString(3, date.substring(4, 6));
                    pstmt.setBigDecimal(4, allOpening);
                    pstmt.setBigDecimal(5, allCount);
                    pstmt.setBigDecimal(6, allAmount);
                    pstmt.setBigDecimal(7, allPeople);

                    pstmt.addBatch();
                    count.getAndIncrement();
                }
                pstmt.executeBatch();

                Optional<Integer> updtCount = Utils.getUpdtCount(tableName);
                if (!updtCount.isPresent()) {
                    throw new Exception("getUpdtCount error");
                }

                schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count.get(), count.get() - updtCount.get(), updtCount.get()));

                flagService.create(new MonthlyCollectionFlag(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM")), tableName, true));
            }
        });
    }
}
