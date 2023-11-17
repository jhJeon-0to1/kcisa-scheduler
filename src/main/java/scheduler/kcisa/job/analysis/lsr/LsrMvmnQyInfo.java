package scheduler.kcisa.job.analysis.lsr;

import org.springframework.scheduling.quartz.QuartzJobBean;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.MartSchedulerLogService;
import scheduler.kcisa.utils.Utils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class LsrMvmnQyInfo extends QuartzJobBean {
    String tableName = "LSR_MVMN_QY_INFO";
    Connection conn;
    MartSchedulerLogService martSchedulerLogService;
    DataSource dataSource;

    public LsrMvmnQyInfo(DataSource dataSource, MartSchedulerLogService martSchedulerLogService) {
        this.dataSource = dataSource;
        this.martSchedulerLogService = martSchedulerLogService;
    }

    @Override
    protected void executeInternal(org.quartz.JobExecutionContext jobExecutionContext) throws org.quartz.JobExecutionException {
        String groupName = jobExecutionContext.getJobDetail().getKey().getGroup();
        String jobName = jobExecutionContext.getJobDetail().getKey().getName();

        try {
            conn = dataSource.getConnection();

            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            String query = Utils.getSQLString("src/main/resources/sql/analysis/lsr/LsrMvmnQyInfo.sql");

            PreparedStatement pstmt = conn.prepareStatement(query);

            int count = pstmt.executeUpdate();

            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count));
        } catch (Exception e) {
            e.printStackTrace();
            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        } finally {
            try {
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
