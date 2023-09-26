package scheduler.kcisa.job.mart.sports;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import javax.sql.DataSource;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.MartSchedulerLogService;
import scheduler.kcisa.utils.Utils;

@Component
public class SportsMatchInfoJob extends QuartzJobBean {
    DataSource dataSource;
    MartSchedulerLogService martSchedulerLogService;
    Connection connection;
    String tableName = "SPORTS_MATCH_INFO";

    @Autowired
    public SportsMatchInfoJob(DataSource dataSource, MartSchedulerLogService martSchedulerLogService) {
        this.dataSource = dataSource;
        this.martSchedulerLogService = martSchedulerLogService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {

        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();

        LocalDate stdDate = LocalDate.now().minusDays(2);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        try {
            connection = dataSource.getConnection();

            martSchedulerLogService
                    .create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            String query = Utils.getSQLString("src/main/resources/sql/mart/sports/SportsMatchInfo.sql");

            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, stdDateStr);

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
