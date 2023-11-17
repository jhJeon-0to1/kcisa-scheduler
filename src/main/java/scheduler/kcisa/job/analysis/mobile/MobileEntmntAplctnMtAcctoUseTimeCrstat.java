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
public class MobileEntmntAplctnMtAcctoUseTimeCrstat extends QuartzJobBean {
    DataSource dataSource;
    MartSchedulerLogService martSchedulerLogService;
    String table = "MOBILE_ENTMNT_APLCTN_MT_ACCTO_USE_TIME_CRSTAT";
    Connection connection;

    public MobileEntmntAplctnMtAcctoUseTimeCrstat(DataSource dataSource, MartSchedulerLogService martSchedulerLogService) {
        this.dataSource = dataSource;
        this.martSchedulerLogService = martSchedulerLogService;
    }

    @Override
    protected void executeInternal(org.quartz.JobExecutionContext jobExecutionContext) throws org.quartz.JobExecutionException {
        String groupName = jobExecutionContext.getJobDetail().getKey().getGroup();
        String jobName = jobExecutionContext.getJobDetail().getKey().getName();

        LocalDate startDate = LocalDate.now().minusMonths(1).withDayOfMonth(1);
        LocalDate endDate = LocalDate.now().minusMonths(1).withDayOfMonth(startDate.lengthOfMonth());
        String startDateStr = startDate.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        String endDateStr = endDate.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));

        try {
            connection = dataSource.getConnection();

            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, table, SchedulerStatus.STARTED));
            
            String query = Utils.getSQLString("src/main/resources/sql/analysis/mobile/MobileEntmntAplctnMtAcctoUseTimeCrstat.sql");

            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, startDateStr);
            pstmt.setString(2, endDateStr);

            int result = pstmt.executeUpdate();

            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, table, SchedulerStatus.SUCCESS, result));
        } catch (Exception e) {
            e.printStackTrace();
            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, table, SchedulerStatus.FAILED, e.getMessage()));
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
