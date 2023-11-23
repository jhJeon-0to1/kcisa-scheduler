package scheduler.kcisa.job.analysis.pblprfr.activate;

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
public class PblprfrMtAcctoGenreActtoRasngCutinCrstatJob extends QuartzJobBean {
    MonthlyAnalysisFlagService flagService;

    List<String> colctList = new ArrayList<>(Arrays.asList("colct_pblprfr_viewng_mt_accto_ctprvn_accto_stats"));

    String tableName = "pblprfr_mt_actto_genre_accto_rasng_cutin_crstat";

    public PblprfrMtAcctoGenreActtoRasngCutinCrstatJob(MonthlyAnalysisFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        LocalDate stdDate = LocalDate.now().minusMonths(1);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));

        JobUtils.executeAnalysisJob(context, tableName, colctList, flagDate, ScheduleInterval.MONTHLY, jobData -> {
            Connection connection = jobData.conn;
            MartSchedulerLogService martSchedulerLogService = (MartSchedulerLogService) jobData.logService;

            String query = Utils.getSQLString("src/main/resources/sql/analysis/pblprfr/PblprfrMtAcctoGenreActtoRasngCutinCrstat.sql");

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
