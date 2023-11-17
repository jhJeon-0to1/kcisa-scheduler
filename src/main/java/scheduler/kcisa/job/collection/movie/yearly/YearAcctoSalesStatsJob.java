package scheduler.kcisa.job.collection.movie.yearly;

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
import scheduler.kcisa.utils.JobUtils;
import scheduler.kcisa.utils.Utils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;

@Component
public class YearAcctoSalesStatsJob extends QuartzJobBean {
    WebClient webClient = WebClient.builder().baseUrl("https://www.kobis.or.kr").build();
    String tableName = "COLCT_MOVIE_YEAR_ACCTO_SALES_STATS";


    @Override
    protected void executeInternal(JobExecutionContext context) {
        JobUtils.executeJob(context, tableName, jobData -> {
            String groupName = jobData.groupName;
            String jobName = jobData.jobName;
            int count = 0;

            Connection connection = jobData.conn;
            String query = Utils.getSQLString("src/main/resources/sql/collection/movie/YearAcctoSalesStats.sql");

            PreparedStatement pstmt = connection.prepareStatement(query);

            String formData = "loadVal=0&searchType=search";
            String response = webClient.post().uri("/kobis/business/stat/them/findYearlyTotalList.do").contentType(MediaType.APPLICATION_FORM_URLENCODED).bodyValue(formData).retrieve().bodyToMono(String.class).block();

            if (response == null) {
                jobData.logService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, "HTML 문서가 null 입니다."));
                return;
            }

            Document html = Jsoup.parse(response);
            List<Element> rows = html.select("tbody > tr");

            for (Element row : rows) {
                List<Element> cells = row.select("td");
                if (cells.get(0).text().equals("합계")) {
                    continue;
                }
                String thisYear = cells.get(0).text().replace("-", "");
                BigDecimal allOpening = new BigDecimal(cells.get(11).text().replace(",", ""));
                BigDecimal allCount = new BigDecimal(cells.get(12).text().replace(",", ""));
                BigDecimal allAmount = new BigDecimal(cells.get(13).text().replace(",", ""));
                BigDecimal allPeople = new BigDecimal(cells.get(14).text().replace(",", ""));

                pstmt.setString(1, thisYear);
                pstmt.setBigDecimal(2, allOpening);
                pstmt.setBigDecimal(3, allCount);
                pstmt.setBigDecimal(4, allAmount);
                pstmt.setBigDecimal(5, allPeople);

                pstmt.addBatch();
                count++;
            }
            pstmt.executeBatch();

            Optional<Integer> updtCount = Utils.getUpdtCount(tableName);
            if (!updtCount.isPresent()) {
                throw new Exception("getUpdtCount error");
            }

            jobData.logService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count, count - updtCount.get(), updtCount.get()));
        });
    }
}
