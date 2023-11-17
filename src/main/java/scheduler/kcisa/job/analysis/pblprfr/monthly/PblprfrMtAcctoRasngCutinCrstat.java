package scheduler.kcisa.job.analysis.pblprfr.monthly;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.utils.JobUtils;
import scheduler.kcisa.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Component
public class PblprfrMtAcctoRasngCutinCrstat extends QuartzJobBean {
    String tableName = "PBLPRFR_MT_ACCTO_RASNG_CUTIN_CRSTAT";

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        LocalDate stdYM = LocalDate.now().minusMonths(1).withDayOfMonth(1);
        String stdYmStr = stdYM.format(DateTimeFormatter.ofPattern("yyyyMM"));

        JobUtils.executeAnalysisJob(context, tableName, stdYmStr, jobData -> {
            Connection connection = jobData.conn;
            String query = Utils.getSQLString("src/main/resources/sql/analysis/pblprfr/PblprfrMtAcctoRasngCutinCrstat.sql");

            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, stdYmStr);
            pstmt.setString(2, stdYmStr);

            int result = pstmt.executeUpdate();

            if (result > 0) {
                jobData.logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, jobData.tableName, SchedulerStatus.SUCCESS, result));
            } else {
                jobData.logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, jobData.tableName, SchedulerStatus.FAILED, stdYmStr + "의 데이터 수집이 안되어 있습니다."));
            }
        });
    }
}
