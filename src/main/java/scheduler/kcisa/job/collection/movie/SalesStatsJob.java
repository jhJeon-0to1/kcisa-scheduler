package scheduler.kcisa.job.collection.movie;

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
public class SalesStatsJob extends QuartzJobBean {
    DataSource dataSource;
    SchedulerLogService schedulerLogService;
    WebClient webClient = WebClient.builder().baseUrl("https://www.kobis.or.kr").build();
    Connection connection;

    public SalesStatsJob(DataSource dataSource, SchedulerLogService schedulerLogService) {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        String gruopName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();
        String tableName = "COLCT_MOVIE_SALES_STATS";

        LocalDate stdDate = LocalDate.now().minusDays(2);
        String dateStr = stdDate.toString().replace("-", "");
        String year = dateStr.substring(0, 4);
        String month = dateStr.substring(4, 6);

        int count = 0;
        try {
            schedulerLogService.create(new SchedulerLog(gruopName, jobName, tableName, SchedulerStatus.STARTED));

            connection = dataSource.getConnection();
            String insertQuery = Utils.getSQLString("src/main/resources/sql/collection/movie/SalesStats.sql");
            PreparedStatement pstmt = connection.prepareStatement(insertQuery);


            String formData = "loadVal=0&searchType=search&selectYear=" + year + "&selectMonth=" + month;
            String response = webClient.post().uri("/kobis/business/stat/them/findDailyTotalList.do").contentType(MediaType.APPLICATION_FORM_URLENCODED).bodyValue(formData).retrieve().bodyToMono(String.class).block();

            if (response == null) {
                schedulerLogService.create(new SchedulerLog(gruopName, jobName, tableName, SchedulerStatus.FAILED, "response is null"));
                return;
            }

            Document html = Jsoup.parse(response);
            List<Element> rows = html.select("tbody > tr");
            for (Element row : rows) {
                List<Element> cells = row.select("td");
                if (cells.get(0).text().equals("합계")) {
                    continue;
                }
                String date = LocalDate.parse(cells.get(0).text()).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                BigDecimal all_opening = new BigDecimal(cells.get(11).text().replace(",", ""));
                BigDecimal all_count = new BigDecimal(cells.get(12).text().replace(",", ""));
                BigDecimal all_amount = new BigDecimal(cells.get(13).text().replace(",", ""));
                BigDecimal all_people = new BigDecimal(cells.get(14).text().replace(",", ""));

                pstmt.setString(1, date);
                pstmt.setString(2, date.substring(0, 4));
                pstmt.setString(3, date.substring(4, 6));
                pstmt.setString(4, date.substring(6, 8));
                pstmt.setBigDecimal(5, all_opening);
                pstmt.setBigDecimal(6, all_count);
                pstmt.setBigDecimal(7, all_amount);
                pstmt.setBigDecimal(8, all_people);

                pstmt.addBatch();
                count++;
            }
            pstmt.executeBatch();

            Optional<Integer> updtCount = Utils.getUpdtCount(tableName);
            if (!updtCount.isPresent()) {
                throw new Exception("getUpdtCount error");
            }

            schedulerLogService.create(new SchedulerLog(gruopName, jobName, tableName, SchedulerStatus.SUCCESS, count, count - updtCount.get(), updtCount.get()));
        } catch (Exception e) {
            e.printStackTrace();
            schedulerLogService.create(new SchedulerLog(gruopName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
