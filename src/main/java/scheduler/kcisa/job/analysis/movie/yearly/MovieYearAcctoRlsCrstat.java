package scheduler.kcisa.job.analysis.movie.yearly;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.flag.analysis.YearlyAnalysisFlag;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.MartSchedulerLogService;
import scheduler.kcisa.service.flag.analysis.YearlyAnalysisFlagService;
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
public class MovieYearAcctoRlsCrstat extends QuartzJobBean {
    List<String> checkList = new ArrayList<>(Arrays.asList("colct_movie_year_accto_ctprvn_accto_stats", "colct_movie_year_accto_sales_stats", "colct_movie_info"));
    YearlyAnalysisFlagService flagService;
    String tableName = "MOVIE_YEAR_ACCTO_RLS_CRSTAT".toLowerCase();

    public MovieYearAcctoRlsCrstat(YearlyAnalysisFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        LocalDate stdDate = LocalDate.now().minusYears(1);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyy"));
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy"));

        JobUtils.executeAnalysisJob(context, tableName, checkList, flagDate, ScheduleInterval.YEARLY, jobData -> {
            Connection connection = jobData.conn;
            MartSchedulerLogService logService = (MartSchedulerLogService) jobData.logService;
            String query = Utils.getSQLString("src/main/resources/sql/analysis/movie/MovieYearAcctoRlsCrstat.sql");

            try (PreparedStatement pstmt = connection.prepareStatement(query);) {
                pstmt.setString(1, stdDateStr);
                pstmt.setString(2, stdDateStr);
                pstmt.setString(3, stdDateStr);

                int count = pstmt.executeUpdate();

                logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.SUCCESS, count));

                flagService.create(new YearlyAnalysisFlag(flagDate, tableName, true));
            }

        });
    }
}
