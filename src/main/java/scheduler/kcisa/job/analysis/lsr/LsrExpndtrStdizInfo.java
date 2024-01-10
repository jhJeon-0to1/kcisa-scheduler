package scheduler.kcisa.job.analysis.lsr;

import org.springframework.scheduling.quartz.QuartzJobBean;
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

public class LsrExpndtrStdizInfo extends QuartzJobBean {
    List<String> checkList = new ArrayList<>(Arrays.asList("colct_lsr_expndtr_stdiz_info"));
    DailyAnalysisFlagService flagService;
    String tableName = "LSR_EXPNDTR_STDIZ_INFO".toLowerCase();

    public LsrExpndtrStdizInfo(DailyAnalysisFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(org.quartz.JobExecutionContext jobExecutionContext) throws org.quartz.JobExecutionException {
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        JobUtils.executeAnalysisJob(jobExecutionContext, tableName, checkList, flagDate, ScheduleInterval.DAILY, jobData -> {
            Connection conn = jobData.conn;
            MartSchedulerLogService logService = (MartSchedulerLogService) jobData.logService;
            String query = Utils.getSQLString("src/main/resources/sql/analysis/lsr/LsrExpndtrStdizInfo.sql");
            String irdsQuery = Utils.getSQLString("src/main/resources/sql/analysis/lsr/LsrExpndtrStdizIrdsRt.sql");

            try (PreparedStatement pstmt = conn.prepareStatement(query); PreparedStatement IrdsPstmt = conn.prepareStatement(irdsQuery);) {
                int count = pstmt.executeUpdate();
                IrdsPstmt.executeUpdate();

                logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.SUCCESS, count));

                flagService.create(new DailyAnalysisFlag(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")), tableName, true));
            }
        });
    }
}
