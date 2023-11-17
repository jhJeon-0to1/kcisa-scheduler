package scheduler.kcisa.job.analysis.mobile;

import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.MartSchedulerLogService;
import scheduler.kcisa.utils.Utils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;

@Component
public class MobileAplctnUseTimeCrstat extends QuartzJobBean {
    DataSource dataSource;
    MartSchedulerLogService martSchedulerLogService;
    String tableName = "MOBILE_APLCTN_USE_TIME_CRSTAT";
    Connection connection;

    public MobileAplctnUseTimeCrstat(DataSource dataSource, MartSchedulerLogService martSchedulerLogService) {
        this.dataSource = dataSource;
        this.martSchedulerLogService = martSchedulerLogService;
    }

    @Override
    protected void executeInternal(org.quartz.JobExecutionContext jobExecutionContext) throws org.quartz.JobExecutionException {
        String groupName = jobExecutionContext.getJobDetail().getKey().getGroup();
        String jobName = jobExecutionContext.getJobDetail().getKey().getName();

        LocalDate stdDate = LocalDate.now().minusDays(3);
        String stdDateStr = stdDate.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
//        String stdDateStr = "20230930";

        try {
            connection = dataSource.getConnection();

            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            String query = Utils.getSQLString("src/main/resources/sql/analysis/mobile/MobileAplctnUseTimeCrstat.sql");

            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, stdDateStr);
            pstmt.setString(2, stdDateStr);

            int count = pstmt.executeUpdate();

            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count));
        } catch (Exception e) {
            e.printStackTrace();
            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
