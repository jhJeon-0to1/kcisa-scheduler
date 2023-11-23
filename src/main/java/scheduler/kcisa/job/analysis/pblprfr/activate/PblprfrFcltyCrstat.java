package scheduler.kcisa.job.analysis.pblprfr.activate;

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
public class PblprfrFcltyCrstat extends QuartzJobBean {
    MonthlyAnalysisFlagService analysisFlagService;
    List<String> checkList = new ArrayList<>(Arrays.asList("colct_pblprfr_fclty_info", "colct_pblprfr_fclty_detail_info"));

    public PblprfrFcltyCrstat(MonthlyAnalysisFlagService analysisFlagService) {
        this.analysisFlagService = analysisFlagService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String tableName = "pblprfr_fclty_crstat";

        LocalDate now = LocalDate.now();
        String ym = now.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));

        LocalDate baseDate = LocalDate.now().withDayOfMonth(1).minusMonths(1);
        String baseDateStr = baseDate.format(DateTimeFormatter.ofPattern("yyyyMM"));

        JobUtils.executeAnalysisJob(context, tableName, checkList, flagDate, ScheduleInterval.MONTHLY, jobData -> {
            String groupName = jobData.groupName;
            String jobName = jobData.jobName;
            Connection connection = jobData.conn;
            MartSchedulerLogService martSchedulerLogService = (MartSchedulerLogService) jobData.logService;

            String query = Utils.getSQLString("src/main/resources/sql/analysis/pblprfr/PblbrfrFcltyCrstat.sql");

            try (PreparedStatement pstmt = connection.prepareStatement(query);) {
                pstmt.setString(1, baseDateStr);
                pstmt.setString(2, ym);

                int count = pstmt.executeUpdate();

                martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count));

                analysisFlagService.create(new MonthlyAnalysisFlag(flagDate, tableName, true));
            }
        });
    }
}
