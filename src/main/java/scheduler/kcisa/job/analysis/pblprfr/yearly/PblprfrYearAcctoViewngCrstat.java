package scheduler.kcisa.job.analysis.pblprfr.yearly;

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
public class PblprfrYearAcctoViewngCrstat extends QuartzJobBean {
    String tableName = "pblprfr_year_accto_viewng_crstat";
    List<String> checkList = new ArrayList<>(Arrays.asList("colct_pblprfr_viewng_year_accto_ctprvn_accto_stats", "ctprvn_accto_popltn_info"));
    YearlyAnalysisFlagService flagService;

    public PblprfrYearAcctoViewngCrstat(YearlyAnalysisFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        LocalDate stdYear = LocalDate.now().minusYears(1).withDayOfYear(1);
        String stdYearStr = stdYear.format(DateTimeFormatter.ofPattern("yyyy"));
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy"));

        JobUtils.executeAnalysisJob(context, tableName, checkList, flagDate, ScheduleInterval.YEARLY, jobData -> {
            Connection connection = jobData.conn;
            MartSchedulerLogService logService = (MartSchedulerLogService) jobData.logService;
            String query = Utils.getSQLString("src/main/resources/sql/analysis/pblprfr/PblprfrYearAcctoViewngCrstat.sql");

            try (PreparedStatement pstmt = connection.prepareStatement(query);) {
                pstmt.setString(1, stdYearStr);
                pstmt.setString(2, stdYearStr);

                int result = pstmt.executeUpdate();

                logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, jobData.tableName, SchedulerStatus.SUCCESS, result));

                flagService.create(new YearlyAnalysisFlag(flagDate, tableName, true));
            }
        });
    }
}
