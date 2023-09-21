package scheduler.kcisa.job.collection.kobis;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.quartz.JobExecutionContext;
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
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

@Component
public class KobisDailyJob extends QuartzJobBean {
    DataSource dataSource;
    SchedulerLogService schedulerLogService;
    WebClient webClient = WebClient.builder().baseUrl("https://www.kobis.or.kr").build();
    Connection connection;

    @Autowired
    public KobisDailyJob(DataSource dataSource, SchedulerLogService schedulerLogService) {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        String gruopName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();
        String tableName = "kobis_일별매출액";
        int count = 0;
        try {
            schedulerLogService.create(new SchedulerLog(gruopName, jobName, tableName, SchedulerStatus.STARTED));

            connection = dataSource.getConnection();
            String insertQuery = "INSERT INTO kcisa.kobis_일별매출액 (date, kr_opening, kr_count, kr_amount, kr_people, kr_share, fr_opening, fr_count, fr_amount, fr_people, fr_share, all_opening, all_count, all_amount, all_people, crt_dt) VALUE (?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?, current_timestamp) ON DUPLICATE KEY UPDATE kr_opening= VALUES(kr_opening), kr_count= VALUES(kr_count), kr_amount= VALUES(kr_amount), kr_people= VALUES(kr_people), kr_share= VALUES(kr_share), fr_opening= VALUES(fr_opening), fr_count= VALUES(fr_count), fr_amount= VALUES(fr_amount), fr_people= VALUES(fr_people), fr_share= VALUES(fr_share), all_opening= VALUES(all_opening), all_count= VALUES(all_count), all_amount= VALUES(all_amount), all_people= VALUES(all_people), updt_dt= current_timestamp";
            PreparedStatement pstmt = connection.prepareStatement(insertQuery);

            LocalDate stdDate = LocalDate.now().minusDays(2);
            String dateStr = stdDate.toString().replace("-", "");
            String year = dateStr.substring(0, 4);
            String month = dateStr.substring(4, 6);

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
                BigDecimal kr_opening = new BigDecimal(cells.get(1).text().replace(",", ""));
                BigDecimal kr_count = new BigDecimal(cells.get(2).text().replace(",", ""));
                BigDecimal kr_amount = new BigDecimal(cells.get(3).text().replace(",", ""));
                BigDecimal kr_people = new BigDecimal(cells.get(4).text().replace(",", ""));
                BigDecimal kr_share = new BigDecimal(cells.get(5).text().replace("%", ""));
                BigDecimal fr_opening = new BigDecimal(cells.get(6).text().replace(",", ""));
                BigDecimal fr_count = new BigDecimal(cells.get(7).text().replace(",", ""));
                BigDecimal fr_amount = new BigDecimal(cells.get(8).text().replace(",", ""));
                BigDecimal fr_people = new BigDecimal(cells.get(9).text().replace(",", ""));
                BigDecimal fr_share = new BigDecimal(cells.get(10).text().replace("%", ""));
                BigDecimal all_opening = new BigDecimal(cells.get(11).text().replace(",", ""));
                BigDecimal all_count = new BigDecimal(cells.get(12).text().replace(",", ""));
                BigDecimal all_amount = new BigDecimal(cells.get(13).text().replace(",", ""));
                BigDecimal all_people = new BigDecimal(cells.get(14).text().replace(",", ""));

                pstmt.setString(1, date);
                pstmt.setBigDecimal(2, kr_opening);
                pstmt.setBigDecimal(3, kr_count);
                pstmt.setBigDecimal(4, kr_amount);
                pstmt.setBigDecimal(5, kr_people);
                pstmt.setBigDecimal(6, kr_share);
                pstmt.setBigDecimal(7, fr_opening);
                pstmt.setBigDecimal(8, fr_count);
                pstmt.setBigDecimal(9, fr_amount);
                pstmt.setBigDecimal(10, fr_people);
                pstmt.setBigDecimal(11, fr_share);
                pstmt.setBigDecimal(12, all_opening);
                pstmt.setBigDecimal(13, all_count);
                pstmt.setBigDecimal(14, all_amount);
                pstmt.setBigDecimal(15, all_people);

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
        }
    }
}
