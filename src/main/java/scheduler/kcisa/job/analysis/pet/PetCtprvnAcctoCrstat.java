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

public class PetCtprvnAcctoCrstat extends QuartzJobBean {
    String tableName = "pet_ctprvn_accto_crstat";
    MonthlyAnalysisFlagService flagService;
    List<String> checkList = new ArrayList<>(Arrays.asList("colct_pet_regist_crstat",
            "colct_pet_hspt_license_info",
            "colct_pet_bty_fclty_license_info",
            "colct_pet_consgn_manage_fclty_license_info",
            "ctprvn_accto_popltn_info"));


    public PetCtprvnAcctoCrstat(MonthlyAnalysisFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(org.quartz.JobExecutionContext jobExecutionContext) throws org.quartz.JobExecutionException {
        LocalDate stdDate = LocalDate.now().minusMonths(1).withDayOfMonth(1);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));

        JobUtils.executeAnalysisJob(jobExecutionContext, tableName, checkList, flagDate, ScheduleInterval.MONTHLY, jobData -> {
            String ctprvnAcctoCrstatQuery = Utils.getSQLString("src/main/resources/sql/analysis/pet/PetCtprvnAcctoCrstat.sql");
            MartSchedulerLogService logService = (MartSchedulerLogService) jobData.logService;

            try (
                    PreparedStatement ctprvnAcctoCrstatPstmt = jobData.conn.prepareStatement(ctprvnAcctoCrstatQuery);) {
                ctprvnAcctoCrstatPstmt.setString(1, stdDateStr);
                ctprvnAcctoCrstatPstmt.setString(2, stdDateStr);
                ctprvnAcctoCrstatPstmt.setString(3, stdDateStr);
                ctprvnAcctoCrstatPstmt.setString(4, stdDateStr);

                int result = ctprvnAcctoCrstatPstmt.executeUpdate();

                logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.SUCCESS, result));

                flagService.create(new MonthlyAnalysisFlag(flagDate, tableName, true));
            }
        });
    }
}
