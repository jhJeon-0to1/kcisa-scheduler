package scheduler.kcisa.job.collection.movie;

import com.fasterxml.jackson.databind.JsonNode;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.model.flag.collection.MonthlyCollectionFlag;
import scheduler.kcisa.service.SchedulerLogService;
import scheduler.kcisa.service.flag.collection.MonthlyCollectionFlagService;
import scheduler.kcisa.utils.JobUtils;
import scheduler.kcisa.utils.ScheduleInterval;
import scheduler.kcisa.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class InfoJob extends QuartzJobBean {
    MonthlyCollectionFlagService flagService;
    WebClient webClient = WebClient.builder().baseUrl("https://www.kobis.or.kr").build();
    @Value("${kobis.api.key}")
    String key;

    public InfoJob(MonthlyCollectionFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        String tableName = "colct_movie_info";
        AtomicInteger count = new AtomicInteger();

        JobUtils.executeJob(context, tableName, jobData -> {
            Connection connection = jobData.conn;
            String groupName = jobData.groupName;
            String jobName = jobData.jobName;
            SchedulerLogService schedulerLogService = (SchedulerLogService) jobData.logService;

            String insertQuery = Utils.getSQLString("src/main/resources/sql/collection/movie/Info.sql");
            try (PreparedStatement pstmt = connection.prepareStatement(insertQuery)) {
                String year = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy"));

                int page = 1;
                while (true) {
                    String url = "/kobisopenapi/webservice/rest/movie/searchMovieList.json?key=" + key + "&curPage=" + page + "&itemPerPage=100&openStartDt=" + year + "&openEndDt=" + year;

                    JsonNode response = webClient.get().uri(url).retrieve().bodyToMono(JsonNode.class).block();
                    if (response == null) {
                        schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, year + " response is null"));
                        return;
                    }

                    int totalCount = response.get("movieListResult").get("totCnt").asInt();
                    if (totalCount == 0) {
                        break;
                    }

                    JsonNode movieList = response.get("movieListResult").get("movieList");
                    for (JsonNode movie : movieList) {
                        String movieCd = movie.get("movieCd").asText();
                        String movieNm = movie.get("movieNm").asText();
                        String movieNmEn = movie.get("movieNmEn").asText();
                        String prdtYear = movie.get("prdtYear").asText();
                        String openDt = movie.get("openDt").asText();
                        String nationAlt = movie.get("nationAlt").asText();
                        String genreAlt = movie.get("genreAlt").asText();
                        String repNationNm = movie.get("repNationNm").asText();
                        String repGenreNm = movie.get("repGenreNm").asText();

                        pstmt.setString(1, movieCd);
                        pstmt.setString(2, movieNm);
                        pstmt.setString(3, movieNmEn);
                        pstmt.setString(4, prdtYear);
                        pstmt.setString(5, openDt);
                        pstmt.setString(6, nationAlt);
                        pstmt.setString(7, genreAlt);
                        pstmt.setString(8, repNationNm);
                        pstmt.setString(9, repGenreNm);

                        pstmt.addBatch();
                        count.getAndIncrement();
                    }
                    page++;
                }
                pstmt.executeBatch();

                Optional<Integer> updtCount = Utils.getUpdtCount(tableName);
                if (!updtCount.isPresent()) {
                    throw new Exception("getUpdtCount error");
                }

                schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count.get(), count.get() - updtCount.get(), updtCount.get()));

                String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));
                String isExist = JobUtils.checkCollectFlag(new ArrayList<>(Collections.singletonList(tableName)), flagDate, ScheduleInterval.MONTHLY);
                if (isExist != null) {
                    flagService.deleteByDateAndTableName(flagDate, tableName);
                }
                flagService.create(new MonthlyCollectionFlag(flagDate, tableName, true));
            }
        });
    }
}
