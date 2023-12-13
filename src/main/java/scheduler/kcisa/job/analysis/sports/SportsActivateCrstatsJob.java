package scheduler.kcisa.job.analysis.sports;

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
public class SportsActivateCrstatsJob extends QuartzJobBean {
    List<String> checkList = new ArrayList<>(Arrays.asList("ctprvn_accto_popltn_info"));
    List<String> checkDataList = new ArrayList<>(Arrays.asList("colct_sports_viewng_info"));
    MonthlyAnalysisFlagService flagService;
    String tableName = "SPORTS_ACTIVATE_CRSTAT".toLowerCase();

    public SportsActivateCrstatsJob(MonthlyAnalysisFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        LocalDate stdDate = LocalDate.now().minusMonths(1);
        LocalDate startDate = stdDate.withDayOfMonth(1);
        LocalDate endDate = stdDate.withDayOfMonth(stdDate.lengthOfMonth());
        String startDateStr = startDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String endDateStr = endDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));
        JobUtils.executeAnalysisJob(context, tableName, checkList, flagDate, ScheduleInterval.MONTHLY, jobData -> {
            Connection connection = jobData.conn;
            MartSchedulerLogService logService = (MartSchedulerLogService) jobData.logService;

            for (LocalDate date = startDate; date.isBefore(endDate.plusDays(1)); date = date.plusDays(1)) {
                String dateStr = date.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                String checkName = JobUtils.checkCollectFlag(checkDataList, dateStr, ScheduleInterval.DAILY);
                if (checkName != null) {
                    logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.FAILED, checkName + " " + dateStr + "의 데이터" + "수집이 완료되지 않았습니다."));
                    return;
                }
            }

            String query = Utils.getSQLString("src/main/resources/sql/analysis/sports/SportsActivateCrstats.sql");

            try (PreparedStatement pstmt = connection.prepareStatement(query);) {
                pstmt.setString(1, startDateStr);
                pstmt.setString(2, endDateStr);
                pstmt.setString(3, startDateStr);
                pstmt.setString(4, endDateStr);

                int count = pstmt.executeUpdate();

                logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.SUCCESS, count));

                flagService.create(new MonthlyAnalysisFlag(flagDate, tableName, true));
            }
        });
    }
}
