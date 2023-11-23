package scheduler.kcisa.job.analysis.mobile;

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
public class MobileEntmntAplctnMtAcctoUseTimeCrstat extends QuartzJobBean {
    List<String> checkList = new ArrayList<>(Arrays.asList());
    MonthlyAnalysisFlagService flagService;
    String table = "MOBILE_ENTMNT_APLCTN_MT_ACCTO_USE_TIME_CRSTAT".toLowerCase();

    public MobileEntmntAplctnMtAcctoUseTimeCrstat(MonthlyAnalysisFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(org.quartz.JobExecutionContext jobExecutionContext) throws org.quartz.JobExecutionException {
        LocalDate startDate = LocalDate.now().minusMonths(1).withDayOfMonth(1);
        LocalDate endDate = LocalDate.now().minusMonths(1).withDayOfMonth(startDate.lengthOfMonth());
        String startDateStr = startDate.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        String endDateStr = endDate.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));

        JobUtils.executeAnalysisJob(jobExecutionContext, table, checkList, flagDate, ScheduleInterval.MONTHLY, jobData -> {
            Connection connection = jobData.conn;
            MartSchedulerLogService logService = (MartSchedulerLogService) jobData.logService;

            String query = Utils.getSQLString("src/main/resources/sql/analysis/mobile/MobileEntmntAplctnMtAcctoUseTimeCrstat.sql");

            try (PreparedStatement pstmt = connection.prepareStatement(query);) {
                pstmt.setString(1, startDateStr);
                pstmt.setString(2, endDateStr);

                int result = pstmt.executeUpdate();

                logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, table, SchedulerStatus.SUCCESS, result));

                flagService.create(new MonthlyAnalysisFlag(flagDate, table, true));
            }
        });
    }
}
