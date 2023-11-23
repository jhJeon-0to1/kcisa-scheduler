package scheduler.kcisa.job.analysis.movie.monthly;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.flag.analysis.MonthlyAnalysisFlag;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.MartSchedulerLogService;
import scheduler.kcisa.service.flag.analysis.MonthlyAnalysisFlagService;
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
public class MtAcctoMovieViewngCrstatJob extends QuartzJobBean {
    List<String> checkList = new ArrayList<>(Arrays.asList("ctprvn_accto_popltn_info", "colct_movie_mt_accto_ctprvn_accto_stats", "colct_movie_mt_accto_sales_stats"));
    MonthlyAnalysisFlagService flagService;
    String tableName = "movie_mt_accto_viewng_crstat";

    public MtAcctoMovieViewngCrstatJob(MonthlyAnalysisFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        LocalDate stdDate = LocalDate.now().minusMonths(1);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));

        JobUtils.executeAnalysisJob(context, tableName, checkList, flagDate, ScheduleInterval.MONTHLY, jobData -> {
            Connection connection = jobData.conn;
            MartSchedulerLogService martSchedulerLogService = (MartSchedulerLogService) jobData.logService;

            String query = Utils.getSQLString("src/main/resources/sql/analysis/movie/MtAcctoMovieViewngCrstat.sql");

            try (PreparedStatement pstmt = connection.prepareStatement(query);) {
                pstmt.setString(1, stdDateStr);
                pstmt.setString(2, stdDateStr);

                int count = pstmt.executeUpdate();

                martSchedulerLogService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.SUCCESS, count));

                flagService.create(new MonthlyAnalysisFlag(flagDate, tableName, true));
            }
        });
    }
}
