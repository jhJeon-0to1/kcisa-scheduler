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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Component
public class CtrpvnAcctoStatsJob extends QuartzJobBean {
    DataSource dataSource;
    SchedulerLogService schedulerLogService;
    Connection connection;
    WebClient webClient = WebClient.builder().baseUrl("https://www.kobis.or.kr").build();
    List<Region> regionList = new ArrayList<>();

    public CtrpvnAcctoStatsJob(DataSource dataSource, SchedulerLogService schedulerLogService) {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;

        regionList.add(new Region("11", "서울시"));
        regionList.add(new Region("26", "부산시"));
        regionList.add(new Region("27", "대구시"));
        regionList.add(new Region("28", "인천시"));
        regionList.add(new Region("29", "광주시"));
        regionList.add(new Region("30", "대전시"));
        regionList.add(new Region("31", "울산시"));
        regionList.add(new Region("41", "경기도"));
        regionList.add(new Region("51", "강원도"));
        regionList.add(new Region("43", "충청북도"));
        regionList.add(new Region("44", "충청남도"));
        regionList.add(new Region("45", "전라북도"));
        regionList.add(new Region("46", "전라남도"));
        regionList.add(new Region("47", "경상북도"));
        regionList.add(new Region("48", "경상남도"));
        regionList.add(new Region("50", "제주도"));
        regionList.add(new Region("36", "세종시"));
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();
        String tableName = "colct_movie_ctprvn_accto_stats".toUpperCase();
        String url = "/kobis/business/stat/them/findAreaShareList.do";

        LocalDate stdDate = LocalDate.now().minusDays(2);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String formData = "sSearchFrom=" + stdDateStr + "&sSearchTo=" + stdDateStr;

        try {
            int count = 0;
            connection = dataSource.getConnection();
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));
            String insertQuery = Utils.getSQLString("src/main/resources/sql/collection/movie/CtprvnAcctoStats.sql");

            PreparedStatement pstmt = connection.prepareStatement(insertQuery);

            String html = webClient.post().uri(url).contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .bodyValue(formData).retrieve().bodyToMono(String.class).block();

            if (html == null) {
                schedulerLogService.create(
                        new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, "html is null"));
                return;
            }
            Document document = Jsoup.parse(html);
            List<Element> rows = document.select("tbody > tr");

            for (Region region : regionList) {
                Element nowRow = rows.stream()
                        .filter(row -> Objects.requireNonNull(row.selectFirst("td")).text().equals(region.name))
                        .findFirst().orElse(null);
                if (nowRow == null) {
                    BigDecimal zero = new BigDecimal(0);
                    pstmt.setString(1, stdDateStr.replace("-", ""));
                    pstmt.setString(2, stdDate.format(DateTimeFormatter.ofPattern("yyyy")));
                    pstmt.setString(3, stdDate.format(DateTimeFormatter.ofPattern("MM")));
                    pstmt.setString(4, stdDate.format(DateTimeFormatter.ofPattern("dd")));
                    pstmt.setString(5, region.code);
                    pstmt.setString(6, region.code);
                    pstmt.setBigDecimal(7, zero);
                    pstmt.setBigDecimal(8, zero);
                    pstmt.setBigDecimal(9, zero);
                } else {
                    List<Element> cells = nowRow.select("td");
                    BigDecimal all_count = new BigDecimal(cells.get(9).text().replace(",", ""));
                    BigDecimal all_amount = new BigDecimal(cells.get(10).text().replace(",", ""));
                    BigDecimal all_people = new BigDecimal(cells.get(11).text().replace(",", ""));

                    pstmt.setString(1, stdDateStr.replace("-", ""));
                    pstmt.setString(2, stdDate.format(DateTimeFormatter.ofPattern("yyyy")));
                    pstmt.setString(3, stdDate.format(DateTimeFormatter.ofPattern("MM")));
                    pstmt.setString(4, stdDate.format(DateTimeFormatter.ofPattern("dd")));
                    pstmt.setString(5, region.code);
                    pstmt.setString(6, region.code);
                    pstmt.setBigDecimal(7, all_count);
                    pstmt.setBigDecimal(8, all_amount);
                    pstmt.setBigDecimal(9, all_people);
                }
                pstmt.addBatch();
                count++;
            }
            pstmt.executeBatch();

            Optional<Integer> updtCount = Utils.getUpdtCount(tableName);
            if (!updtCount.isPresent()) {
                throw new Exception("getUpdtCount error");
            }

            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count,
                    count - updtCount.get(), updtCount.get()));
        } catch (Exception e) {
            e.printStackTrace();
            schedulerLogService
                    .create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class Region {
        String code;
        String name;

        public Region(String code, String name) {
            this.code = code;
            this.name = name;
        }
    }
}
