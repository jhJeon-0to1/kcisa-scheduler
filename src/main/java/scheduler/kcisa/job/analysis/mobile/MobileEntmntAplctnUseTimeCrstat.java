package scheduler.kcisa.job.analysis.mobile;

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
public class MobileEntmntAplctnUseTimeCrstat extends QuartzJobBean {
    List<String> checkList = new ArrayList<>(Arrays.asList("colct_mobile_ctgry_use_qy_info"));
    DailyAnalysisFlagService flagService;
    String tableName = "MOBILE_ENTMNT_APLCTN_USE_TIME_CRSTAT".toLowerCase();

    public MobileEntmntAplctnUseTimeCrstat(DailyAnalysisFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(org.quartz.JobExecutionContext jobExecutionContext) throws org.quartz.JobExecutionException {
        LocalDate stdDate = LocalDate.now().minusDays(4);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        JobUtils.executeAnalysisJob(jobExecutionContext, tableName, checkList, flagDate, ScheduleInterval.DAILY, jobData -> {
            Connection connection = jobData.conn;
            MartSchedulerLogService logService = (MartSchedulerLogService) jobData.logService;
            String query = Utils.getSQLString("src/main/resources/sql/analysis/mobile/MobileEntmntAplctnUseTimeCrstat.sql");

            try (PreparedStatement pstmt = connection.prepareStatement(query);) {
                pstmt.setString(1, stdDateStr);
                pstmt.setString(2, stdDateStr);

                int count = pstmt.executeUpdate();

                logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.SUCCESS, count));

                flagService.create(new DailyAnalysisFlag(flagDate, tableName, true));
            }
        });


    }
}
