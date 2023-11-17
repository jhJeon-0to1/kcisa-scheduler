package scheduler.kcisa.job.analysis.pblprfr.yearly;

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
public class PblprfrYearAcctoViewngCrstat extends QuartzJobBean {
    String tableName = "PBLPRFR_YEAR_ACCTO_VIEWNG_CRSTAT";

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        LocalDate stdYear = LocalDate.now().minusYears(1).withDayOfYear(1);
        String stdYearStr = stdYear.format(DateTimeFormatter.ofPattern("yyyy"));

        JobUtils.executeAnalysisJob(context, tableName, stdYearStr, jobData -> {
            Connection connection = jobData.conn;
            String query = Utils.getSQLString("src/main/resources/sql/analysis/pblprfr/PblprfrYearAcctoViewngCrstat.sql");

            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, stdYearStr);
            pstmt.setString(2, stdYearStr);

            int result = pstmt.executeUpdate();

            if (result > 0) {
                jobData.logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, jobData.tableName, SchedulerStatus.SUCCESS, result));
            } else {
                jobData.logService.create(new MartSchedulerLog(jobData.groupName, jobData.jobName, jobData.tableName, SchedulerStatus.FAILED, stdYearStr + "의 데이터 수집이 안되어 있습니다."));
            }
        });
    }
}
