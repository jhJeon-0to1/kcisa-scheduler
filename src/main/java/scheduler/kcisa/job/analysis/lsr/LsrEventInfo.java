package scheduler.kcisa.job.analysis.lsr;

import org.springframework.scheduling.quartz.QuartzJobBean;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.flag.collection.DailyCollectionFlag;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.MartSchedulerLogService;
import scheduler.kcisa.service.flag.collection.DailyCollectionFlagService;
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

public class LsrEventInfo extends QuartzJobBean {
    String tableName = "lsr_event_info";
    List<String> checkList = new ArrayList<>(Arrays.asList());
    DailyCollectionFlagService flagService;
    List<String> analysisList = new ArrayList<>(Arrays.asList("lsr_mvmn_qy_info", "lsr_expndtr_stdiz_info"));

    public LsrEventInfo(DailyCollectionFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(org.quartz.JobExecutionContext jobExecutionContext) throws org.quartz.JobExecutionException {
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        JobUtils.executeAnalysisJob(jobExecutionContext, tableName, checkList, flagDate, ScheduleInterval.DAILY, jobData -> {
            Connection conn = jobData.conn;
            MartSchedulerLogService logService = (MartSchedulerLogService) jobData.logService;

            boolean checkAnalysis = JobUtils.checkAnalysisFlag(analysisList, flagDate, ScheduleInterval.DAILY);
            if (!checkAnalysis) {
                logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.FAILED, "분석을 위한 수집이 완료되지 않았습니다."));
                return;
            }

            String expndtrQuery = Utils.getSQLString("src/main/resources/sql/analysis/lsr/LsrEventExpndtrUpdate.sql");
            String mvmnQuery = Utils.getSQLString("src/main/resources/sql/analysis/lsr/LsrEventMvmnUpdate.sql");
            try (PreparedStatement expndtrPstmt = conn.prepareStatement(expndtrQuery); PreparedStatement mvmnPstmt = conn.prepareStatement(mvmnQuery);) {
                int count = expndtrPstmt.executeUpdate();
                count += mvmnPstmt.executeUpdate();

                logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, tableName, SchedulerStatus.SUCCESS, count));

                flagService.create(new DailyCollectionFlag(LocalDate.now(), tableName, true));
            }
        });
    }
}
