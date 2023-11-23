package scheduler.kcisa.job.analysis.movie;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.flag.analysis.DailyAnalysisFlag;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.MartSchedulerLogService;
import scheduler.kcisa.service.flag.analysis.DailyAnalysisFlagService;
import scheduler.kcisa.utils.JobUtils;
import scheduler.kcisa.utils.ScheduleInterval;
import scheduler.kcisa.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class MovieViewngCrstatJob extends QuartzJobBean {
    List<String> checkList = new ArrayList<>(Arrays.asList("colct_movie_sales_stats", "colct_movie_ctprvn_accto_stats"));
    DailyAnalysisFlagService flagService;
    String tableName = "movie_viewng_crstat";

    public MovieViewngCrstatJob(DailyAnalysisFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        LocalDate stdDate = LocalDate.now().minusDays(2);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        JobUtils.executeAnalysisJob(context, tableName, checkList, flagDate, ScheduleInterval.DAILY, jobData -> {
            Connection connection = jobData.conn;
            MartSchedulerLogService martSchedulerLogService = (MartSchedulerLogService) jobData.logService;
            String query = Utils.getSQLString("src/main/resources/sql/analysis/movie/MovieViewngCrstat.sql");

            try (PreparedStatement pstmt = connection.prepareStatement(query);) {
                pstmt.setString(1, stdDateStr);
                pstmt.setString(2, stdDateStr);

                int count = pstmt.executeUpdate();

                martSchedulerLogService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.SUCCESS, count));

                flagService.create(new DailyAnalysisFlag(LocalDate.now(), tableName, true));
            }
        });
    }
}
