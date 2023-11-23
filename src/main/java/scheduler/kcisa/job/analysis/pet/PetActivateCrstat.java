package scheduler.kcisa.job.analysis.pet;

import org.springframework.scheduling.quartz.QuartzJobBean;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.flag.analysis.MonthlyAnalysisFlag;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.MartSchedulerLogService;
import scheduler.kcisa.service.flag.analysis.MonthlyAnalysisFlagService;
import scheduler.kcisa.utils.JobUtils;
import scheduler.kcisa.utils.ScheduleInterval;
import scheduler.kcisa.utils.Utils;

import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class PetActivateCrstat extends QuartzJobBean {
    String tableName = "pet_activate_crstat";
    MonthlyAnalysisFlagService flagService;
    List<String> checkList = new ArrayList<>(Arrays.asList());
    List<String> analysisList = new ArrayList<>(Arrays.asList("pet_ctprvn_accto_crstat"));

    public PetActivateCrstat(MonthlyAnalysisFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(org.quartz.JobExecutionContext jobExecutionContext) throws org.quartz.JobExecutionException {
        LocalDate stdDate = LocalDate.now().minusMonths(1).withDayOfMonth(1);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));

        JobUtils.executeAnalysisJob(jobExecutionContext, tableName, checkList, flagDate, ScheduleInterval.MONTHLY, jobData -> {
            String activateQuery = Utils.getSQLString("src/main/resources/sql/analysis/pet/PetActivateCrstat.sql");
            MartSchedulerLogService logService = (MartSchedulerLogService) jobData.logService;

            boolean checkAnalysis = JobUtils.checkAnalysisFlag(analysisList, flagDate, ScheduleInterval.MONTHLY);
            if (!checkAnalysis) {
                logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.FAILED, "pet_ctprvn_accto_crstat 데이터가 수집이 완료되지 않았습니다."));
                return;
            }

            try (PreparedStatement activatePstmt = jobData.conn.prepareStatement(activateQuery);) {
                activatePstmt.setString(1, stdDateStr);
                activatePstmt.setString(2, stdDateStr);

                int result = activatePstmt.executeUpdate();

                logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.SUCCESS, result));

                flagService.create(new MonthlyAnalysisFlag(flagDate, tableName, true));
            }
        });
    }
}
