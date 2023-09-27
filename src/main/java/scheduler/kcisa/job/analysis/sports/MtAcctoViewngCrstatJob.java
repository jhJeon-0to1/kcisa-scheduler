package scheduler.kcisa.job.analysis.sports;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
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
import java.time.format.DateTimeFormatter;

@Component
public class MtAcctoViewngCrstatJob extends QuartzJobBean {
    DataSource dataSource;
    MartSchedulerLogService martSchedulerLogService;
    Connection connection;
    String tableName = "SPORTS_MT_ACCTO_VIEWNG_CRSTAT";

    @Autowired
    public MtAcctoViewngCrstatJob(DataSource dataSource, MartSchedulerLogService martSchedulerLogService) {
        this.dataSource = dataSource;
        this.martSchedulerLogService = martSchedulerLogService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();

        LocalDate stdDate = LocalDate.now().minusMonths(1);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String stdYear = stdDate.format(DateTimeFormatter.ofPattern("yyyy"));
        String stdMonth = stdDate.format(DateTimeFormatter.ofPattern("MM"));

        try {
            connection = dataSource.getConnection();

            Boolean isExist = Utils.checkAlreadyExist(tableName, stdDateStr, context);

            if (isExist) {
                return;
            }

            String query = Utils.getSQLString("src/main/resources/sql/analysis/sports/MtAcctoViewngCrstat.sql");

            PreparedStatement pstmt = connection.prepareStatement(query);

            pstmt.setString(1, stdYear);
            pstmt.setString(2, stdMonth);
            pstmt.setString(3, stdYear);
            pstmt.setString(4, stdMonth);

            int count = pstmt.executeUpdate();

            martSchedulerLogService.create(
                    new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count));
        } catch (Exception e) {
            e.printStackTrace();
            martSchedulerLogService.create(
                    new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        }
    }
}
