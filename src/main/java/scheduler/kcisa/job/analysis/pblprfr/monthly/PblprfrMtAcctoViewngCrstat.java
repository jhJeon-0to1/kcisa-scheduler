package scheduler.kcisa.job.analysis.pblprfr.monthly;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import scheduler.kcisa.model.SchedulerStatus;
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
public class PblprfrMtAcctoViewngCrstat extends QuartzJobBean {
    String tableName = "PBLPRFR_MT_ACCTO_VIEWNG_CRSTAT";
    List<String> checkList = new ArrayList<>(Arrays.asList("colct_pblprfr_viewng_mt_accto_ctprvn_accto_stats", "ctprvn_accto_popltn_info"));
    MonthlyAnalysisFlagService flagService;

    public PblprfrMtAcctoViewngCrstat(MonthlyAnalysisFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        LocalDate stdYM = LocalDate.now().minusMonths(1).withDayOfMonth(1);
        String stdYmStr = stdYM.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));

        JobUtils.executeAnalysisJob(context, tableName, checkList, flagDate, ScheduleInterval.MONTHLY, jobData -> {
            Connection connection = jobData.conn;
            MartSchedulerLogService logService = (MartSchedulerLogService) jobData.logService;

            String query = Utils.getSQLString("src/main/resources/sql/analysis/pblprfr/PblprfrMtAcctoViewngCrstat.sql");

            try (PreparedStatement pstmt = connection.prepareStatement(query);) {
                pstmt.setString(1, stdYmStr);
                pstmt.setString(2, stdYmStr);

                int result = pstmt.executeUpdate();

                logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, jobData.tableName, SchedulerStatus.SUCCESS, result));
            }
        });
    }
}
