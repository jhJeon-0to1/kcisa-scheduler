package scheduler.kcisa.job.collection.kobis;

import com.fasterxml.jackson.databind.JsonNode;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.service.SchedulerLogService;
import scheduler.kcisa.utils.Utils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.util.Optional;

@Component
public class KobisMovieJob extends QuartzJobBean {
    DataSource dataSource;
    SchedulerLogService schedulerLogService;
    Connection connection;
    WebClient webClient = WebClient.builder().baseUrl("https://www.kobis.or.kr").build();

    @Autowired
    public KobisMovieJob(DataSource dataSource, SchedulerLogService schedulerLogService) {
        this.dataSource = dataSource;
        this.schedulerLogService = schedulerLogService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();
        String tableName = "kobis_movie";

        try {
            int count = 0;
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            connection = dataSource.getConnection();

            String insertQuery = "INSERT INTO kcisa.kobis_movie (movieCd, movieNm, movieNmEn, prdtYear, openDt, typeNm, prdtStatNm, nationAlt, genreAlt, repNationNm, repGenreNm) VALUE (?, ?, ?, ?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE movieNm = VALUES(movieNm), movieNmEn = VALUES(movieNmEn), prdtYear = VALUES(prdtYear), openDt = VALUES(openDt), typeNm = VALUES(typeNm), prdtStatNm = VALUES(prdtStatNm), nationAlt = VALUES(nationAlt), genreAlt = VALUES(genreAlt), repNationNm = VALUES(repNationNm), repGenreNm = VALUES(repGenreNm), updt_dt = NOW()";
            PreparedStatement pstmt = connection.prepareStatement(insertQuery);

            String year = LocalDate.now().minusDays(2).toString().substring(0, 4);
            int page = 1;
            while (true) {
                String url = "/kobisopenapi/webservice/rest/movie/searchMovieList.json?key=b7cd95215c53bf9cd09563af6a1fd3ca&curPage=" + page + "&itemPerPage=100&openStartDt=" + year + "&openEndDt=" + year;

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
                    String typeNm = movie.get("typeNm").asText();
                    String prdtStatNm = movie.get("prdtStatNm").asText();
                    String nationAlt = movie.get("nationAlt").asText();
                    String genreAlt = movie.get("genreAlt").asText();
                    String repNationNm = movie.get("repNationNm").asText();
                    String repGenreNm = movie.get("repGenreNm").asText();

                    pstmt.setString(1, movieCd);
                    pstmt.setString(2, movieNm);
                    pstmt.setString(3, movieNmEn);
                    pstmt.setString(4, prdtYear);
                    pstmt.setString(5, openDt);
                    pstmt.setString(6, typeNm);
                    pstmt.setString(7, prdtStatNm);
                    pstmt.setString(8, nationAlt);
                    pstmt.setString(9, genreAlt);
                    pstmt.setString(10, repNationNm);
                    pstmt.setString(11, repGenreNm);

                    pstmt.addBatch();
                    count++;
                }
                page++;
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
}
