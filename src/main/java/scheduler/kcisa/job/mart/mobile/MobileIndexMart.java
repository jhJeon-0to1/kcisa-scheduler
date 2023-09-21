package scheduler.kcisa.job.mart.mobile;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.MartSchedulerLogService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;

@Component
public class MobileIndexMart extends QuartzJobBean {
    DataSource dataSource;
    MartSchedulerLogService martSchedulerLogService;
    Connection connection;
    String tableName = "DM_MOBILE_이용량";

    @Autowired
    public MobileIndexMart(DataSource dataSource, MartSchedulerLogService martSchedulerLogService) {
        this.dataSource = dataSource;
        this.martSchedulerLogService = martSchedulerLogService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        LocalDate today = LocalDate.now();
        String date = today.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        try {
            String groupName = context.getJobDetail().getKey().getGroup();
            String jobName = context.getJobDetail().getKey().getName();
            connection = dataSource.getConnection();

            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            String query = """
                    INSERT INTO kcisa_mart.DM_MOBILE_이용량
                    (date, year, month, day, categoryMain, categorySub, userTotal, userAos, userIos, timeTotal, timeAos, timeIos, crt_dt, updt_dt)
                    SELECT
                        date
                        , substr(date, 1, 4) as year
                        , substr(date, 5, 2) as month
                        , substr(date, 7, 2) as day
                        , categoryMain
                        , categorySub
                        , userTotal
                        , userAos
                        , userIos
                        , timeTotal
                        , timeAos
                        , timeIos
                        , current_timestamp as crt_dt
                        , null as updt_dt
                    FROM kcisa.mobile_이용량
                    WHERE
                        date = ?
                        """;
            PreparedStatement pstmt = connection.prepareStatement(query);
            
            pstmt.setString(1, date);

            pstmt.executeUpdate();

            int count = pstmt.getUpdateCount();

            System.out.println("MobileIndexMart End" + count);

            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED, count));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
