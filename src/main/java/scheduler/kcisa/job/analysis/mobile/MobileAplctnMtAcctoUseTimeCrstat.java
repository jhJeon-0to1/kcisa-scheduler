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
public class MobileAplctnMtAcctoUseTimeCrstat extends QuartzJobBean {
    List<String> checkList = new ArrayList<>(Arrays.asList());
    MonthlyAnalysisFlagService flagService;
    String tableName = "MOBILE_APLCTN_MT_ACCTO_USE_TIME_CRSTAT".toLowerCase();

    public MobileAplctnMtAcctoUseTimeCrstat(MonthlyAnalysisFlagService analysisFlagService) {
        this.flagService = analysisFlagService;
    }

    @Override
    protected void executeInternal(org.quartz.JobExecutionContext jobExecutionContext) throws org.quartz.JobExecutionException {
        LocalDate startDate = LocalDate.now().minusMonths(1).withDayOfMonth(1);
        LocalDate endDate = LocalDate.now().minusMonths(1).withDayOfMonth(startDate.lengthOfMonth());
        String startDateStr = startDate.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        String endDateStr = endDate.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));

        JobUtils.executeAnalysisJob(jobExecutionContext, tableName, checkList, flagDate, ScheduleInterval.MONTHLY, jobData -> {
            Connection connection = jobData.conn;
            MartSchedulerLogService logService = (MartSchedulerLogService) jobData.logService;
            String query = Utils.getSQLString("src/main/resources/sql/analysis/mobile/MobileAplctnMtAcctoUseTimeCrstat.sql");

            try (PreparedStatement preparedStatement = connection.prepareStatement(query);) {
                preparedStatement.setString(1, startDateStr);
                preparedStatement.setString(2, endDateStr);
                preparedStatement.setString(3, startDateStr.substring(0, 6));
                preparedStatement.setString(4, startDateStr);
                preparedStatement.setString(5, endDateStr);
                preparedStatement.setString(6, startDateStr.substring(0, 6));

                int result = preparedStatement.executeUpdate();

                logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.SUCCESS, result));

                flagService.create(new MonthlyAnalysisFlag(flagDate, tableName, true));
            }
        });
    }
}
