package scheduler.kcisa.job.mart.kobis;

import java.sql.Connection;

import javax.sql.DataSource;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import scheduler.kcisa.service.MartSchedulerLogService;

@Component
public class KobisActivateJob extends QuartzJobBean {
    DataSource dataSource;
    MartSchedulerLogService martSchedulerLogService;
    Connection connection;
    String tableName = "MOIVIE_ACTIVATE_CRSTAT";

    public KobisActivateJob(DataSource dataSource, MartSchedulerLogService martSchedulerLogService) {
        this.dataSource = dataSource;
        this.martSchedulerLogService = martSchedulerLogService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();

        // LocalDate now = LocalDate.now();
        // String ym = now.format(DateTimeFormatter.ofPattern("yyyyMM"));

        // try {
        // connection = dataSource.getConnection();

        // Boolean isExist = Utils.checkAlreadyExist(tableName, ym, context);

        // if (isExist) {
        // return;
        // }

        // String query =
        // Utils.getSQLString("src/main/resources/sql/mart/kopis/KopisPlaceState.sql");

        // PreparedStatement pstmt = connection.prepareStatement(query);
        // pstmt.setString(1, ym);
        // pstmt.setString(2, ym);

        // int count = pstmt.executeUpdate();

        // martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName,
        // tableName, SchedulerStatus.SUCCESS,
        // count));
        // } catch (Exception e) {
        // martSchedulerLogService.create(
        // new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAIL,
        // 0));
        // } finally {

        // }
    }
}
