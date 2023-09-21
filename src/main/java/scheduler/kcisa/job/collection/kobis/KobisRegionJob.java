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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Component
public class KobisRegionJob extends QuartzJobBean {
    DataSource dataSource;
    SchedulerLogService schedulerLogService;
    Connection connection;
    WebClient webClient = WebClient.builder().baseUrl("https://www.kobis.or.kr").build();
    List<Region> regionList = new ArrayList<>();

    @Autowired
    public KobisRegionJob(DataSource dataSource, SchedulerLogService schedulerLogService) {
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
        String tableName = "kobis_지역별일별";
        String url = "/kobis/business/stat/them/findAreaShareList.do";

        LocalDate stdDate = LocalDate.now().minusDays(2);
        String dateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String formData = "sSearchFrom=" + dateStr + "&sSearchTo=" + dateStr;


        try {
            int count = 0;
            connection = dataSource.getConnection();
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));
            String insertQuery = "INSERT INTO kcisa.kobis_지역별일별 (date, region, region_code, kr_count, kr_amount, kr_people, kr_share, fr_count, fr_amount, fr_people, fr_share, all_count, all_amount, all_people, all_share) VALUE (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE kr_count =VALUES(kr_count), kr_amount =VALUES(kr_amount), kr_people =VALUES(kr_people), kr_share =VALUES(kr_share), fr_count =VALUES(fr_count), fr_amount =VALUES(fr_amount), fr_people =VALUES(fr_people), fr_share =VALUES(fr_share), all_count =VALUES(all_count), all_amount =VALUES(all_amount), all_people =VALUES(all_people), all_share =VALUES(all_share), updt_dt = NOW()";

            PreparedStatement pstmt = connection.prepareStatement(insertQuery);

            String html = webClient.post().uri(url).contentType(MediaType.APPLICATION_FORM_URLENCODED).bodyValue(formData).retrieve().bodyToMono(String.class).block();

            if (html == null) {
                schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, "html is null"));
                return;
            }
            Document document = Jsoup.parse(html);
            List<Element> rows = document.select("tbody > tr");

            for (Region region : regionList) {
                Element nowRow = rows.stream().filter(row -> Objects.requireNonNull(row.selectFirst("td")).text().equals(region.name)).findFirst().orElse(null);
                if (nowRow == null) {
                    pstmt.setString(1, dateStr.replace("-", ""));
                    pstmt.setString(2, region.name);
                    pstmt.setString(3, region.code);
                    BigDecimal zero = new BigDecimal(0);
                    pstmt.setBigDecimal(4, zero);
                    pstmt.setBigDecimal(5, zero);
                    pstmt.setBigDecimal(6, zero);
                    pstmt.setBigDecimal(7, zero);
                    pstmt.setBigDecimal(8, zero);
                    pstmt.setBigDecimal(9, zero);
                    pstmt.setBigDecimal(10, zero);
                    pstmt.setBigDecimal(11, zero);
                    pstmt.setBigDecimal(12, zero);
                    pstmt.setBigDecimal(13, zero);
                    pstmt.setBigDecimal(14, zero);
                    pstmt.setBigDecimal(15, zero);
                } else {
                    List<Element> cells = nowRow.select("td");
                    pstmt.setString(1, dateStr.replace("-", ""));
                    pstmt.setString(2, region.name);
                    pstmt.setString(3, region.code);
                    BigDecimal kr_count = new BigDecimal(cells.get(1).text().replace(",", ""));
                    BigDecimal kr_amount = new BigDecimal(cells.get(2).text().replace(",", ""));
                    BigDecimal kr_people = new BigDecimal(cells.get(3).text().replace(",", ""));
                    BigDecimal kr_share = new BigDecimal(cells.get(4).text().replace("%", ""));
                    BigDecimal fr_count = new BigDecimal(cells.get(5).text().replace(",", ""));
                    BigDecimal fr_amount = new BigDecimal(cells.get(6).text().replace(",", ""));
                    BigDecimal fr_people = new BigDecimal(cells.get(7).text().replace(",", ""));
                    BigDecimal fr_share = new BigDecimal(cells.get(8).text().replace("%", ""));
                    BigDecimal all_count = new BigDecimal(cells.get(9).text().replace(",", ""));
                    BigDecimal all_amount = new BigDecimal(cells.get(10).text().replace(",", ""));
                    BigDecimal all_people = new BigDecimal(cells.get(11).text().replace(",", ""));
                    BigDecimal all_share = new BigDecimal(cells.get(12).text().replace("%", ""));
                    pstmt.setBigDecimal(4, kr_count);
                    pstmt.setBigDecimal(5, kr_amount);
                    pstmt.setBigDecimal(6, kr_people);
                    pstmt.setBigDecimal(7, kr_share);
                    pstmt.setBigDecimal(8, fr_count);
                    pstmt.setBigDecimal(9, fr_amount);
                    pstmt.setBigDecimal(10, fr_people);
                    pstmt.setBigDecimal(11, fr_share);
                    pstmt.setBigDecimal(12, all_count);
                    pstmt.setBigDecimal(13, all_amount);
                    pstmt.setBigDecimal(14, all_people);
                    pstmt.setBigDecimal(15, all_share);
                }
                pstmt.addBatch();
                count++;
            }
            pstmt.executeBatch();

            Optional<Integer> updtCount = Utils.getUpdtCount(tableName);
            if (!updtCount.isPresent()) {
                throw new Exception("getUpdtCount error");
            }

            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count, count - updtCount.get(), updtCount.get()));
        } catch (Exception e) {
            e.printStackTrace();
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
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
